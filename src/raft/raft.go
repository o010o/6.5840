package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool   // snapshot valid?
	Snapshot      []byte // data of snapshot
	SnapshotTerm  int    // term of the last entry in snapshot
	SnapshotIndex int    // index of the last entry in snapshot
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!

type RaftStateType uint8
type AppendStateType uint8

const (
	voteForNone             int             = -1  // vote for nobody
	applyInterval           int64           = 10  // 10 milisecond
	heartbeatInterval       int64           = 100 // 100 milisecond
	sendEntriesInterval     int64           = 100 //
	electionMin             int64           = 150 // min election overtime
	electionMax             int64           = 400 //
	tickMilisec             int64           = 10  // 10 milisecond one tick
	Follower                RaftStateType   = 0
	Candidate               RaftStateType   = 1
	Leader                  RaftStateType   = 2
	appendStateSucc         AppendStateType = 0 // append successfully
	appendStateIgnore       AppendStateType = 1 // append message is ignore because of term
	appendStatePrevNotExist AppendStateType = 2 // append failed because previous entry is not exist
	appendStatePrevConflict AppendStateType = 3
	debug                   bool            = false
)

type sendRecord struct {
	mu           sync.RWMutex
	nextIndex    []int   // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex   []int   // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	lastModifies []int64 // last modify time of index
}

func (sei *sendRecord) getNext(id int) int {
	sei.mu.RLock()
	defer sei.mu.RUnlock()

	return sei.nextIndex[id]
}

func (sei *sendRecord) construct(len int) {
	sei.nextIndex = make([]int, len)
	sei.matchIndex = make([]int, len)
	sei.lastModifies = make([]int64, len)
}

func (sei *sendRecord) initialize(index int, t int64) {
	sei.mu.Lock()
	defer sei.mu.Unlock()

	if index == 0 {
		panic("initialize: index should not be zero")
	}

	for i := 0; i < len(sei.matchIndex); i++ {
		sei.nextIndex[i] = index
		sei.matchIndex[i] = 0
		sei.lastModifies[i] = t
	}
}

func (sei *sendRecord) copyMatchIndex() []int {
	sei.mu.RLock()
	defer sei.mu.RUnlock()

	matchIndex := make([]int, len(sei.matchIndex))
	copy(matchIndex, sei.matchIndex)
	return matchIndex
}

func (rf *Raft) updateMatchNextLock(server int, thisTime int64, nextIndex int, matchIndex int) {
	rf.sendRecord.mu.Lock()
	defer rf.sendRecord.mu.Unlock()

	rf.updateMatchNext(server, thisTime, nextIndex, matchIndex)
}

func (rf *Raft) updateMatchNext(server int, thisTime int64, nextIndex int, matchIndex int) {
	// do not allow older Appendentries update next
	if thisTime <= rf.sendRecord.lastModifies[server] && rf.me != server {
		return
	}

	rf.sendRecord.lastModifies[server] = thisTime

	if nextIndex > 0 {
		rf.sendRecord.nextIndex[server] = nextIndex
	}

	if matchIndex > 0 {
		rf.sendRecord.matchIndex[server] = matchIndex
	}

	if debug {
		log.Printf("%v ----> %v, next=%v, match=%v", rf.me, server, rf.sendRecord.nextIndex[server], rf.sendRecord.matchIndex[server])
	}
}

//-------------------------------- Raft -------------------------------------

// A Go object implementing a single Raft peer.
type Raft struct {
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	sendRecord        sendRecord
	term              int           // term of current server
	voteFor           int           // vote for which server this term
	state             RaftStateType // state of current server
	log               raftLog       // content of log entry
	machine           stateMachine  // state machine
	curTime           int64
	nextElect         int64
	lastSendEntries   int64
	lastSendHeartbeat int64
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) curTermLock() int {
	rf.lock()
	defer rf.unlock()

	return rf.term
}

func (rf *Raft) curTerm() int {
	return rf.term
}

func (rf *Raft) setTerm(term int) {
	rf.term = term
}

func (rf *Raft) curState() RaftStateType {
	return rf.state
}

func (rf *Raft) curStateLock() RaftStateType {
	rf.lock()
	defer rf.unlock()

	return rf.curState()
}

func (rf *Raft) setState(state RaftStateType) {
	rf.state = state
}

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) updateCommited(thisTerm int) {
	rf.lock()
	defer rf.unlock()
	// TODO: optimize, could not lock all
	if thisTerm != rf.curTerm() {
		return
	}

	l := len(rf.peers)

	matchIndex := rf.sendRecord.copyMatchIndex()

	sort.Ints(matchIndex)
	// update commited log entry
	if ed, _ := rf.log.getEDLock(matchIndex[l-rf.agreeMin()]); ed.Term == rf.curTerm() {
		ok := rf.log.advanceCommitedLock(ed.Index)
		if ok && debug {
			log.Printf("me=%v, term %v commited %v", rf.me, rf.curTerm(), ed)
		}
	}
}

func (rf *Raft) String() string {
	return fmt.Sprintf("me:%v, term:%v, state:%v", rf.me, rf.curTerm(), rf.state)
}

func (rf *Raft) construct(applyCh chan ApplyMsg) {
	rf.sendRecord.construct(len(rf.peers))
	rf.setTerm(0)
	rf.resetVoteFor()
	rf.setState(Follower)
	rf.curTime = 0
	rf.resetElectTimer(0)

	rf.log.construct()
	rf.machine.construct(applyCh)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unlock()

	// Your code here (3A).
	return rf.curTerm(), rf.curState() == Leader
}

//-------------------------------- Raft -------------------------------------

type entryDescriptor struct {
	Index int
	Term  int
}

func (e *entryDescriptor) getTerm() int {
	return e.Term
}

func (e *entryDescriptor) getIndex() int {
	return e.Index
}

type AppendEntryArgs struct {
	LeaderId      int             // id of the leader who sends this message
	Entries       []logEntry      // Entries need to append into follower
	PreEntry      entryDescriptor // append Entries after PreEntry
	Term          int             // term of server
	CommitedIndex int
}

func (aep AppendEntryArgs) String() string {
	return fmt.Sprintf("preEntry=[%v], entryLen=%v, thisTerm=%v, Commited=%v", aep.PreEntry, len(aep.Entries), aep.Term, aep.CommitedIndex)
}

type ConflictInfo struct {
	Term     int // term of conflict log entry if append failed
	MinIndex int // min index of log entry with term = XTerm if append failed
	LogLen   int // len of peer server log
}

type AppendEntryReply struct {
	State    AppendStateType // 0:succ, 1:lower term, 2:
	Term     int             // term of peer server
	Conflict ConflictInfo    // labgob will abort if using interface{} here for unknown reason
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.lock()
	defer rf.unlock()

	if rf.curTerm() > args.Term {
		(*reply) = AppendEntryReply{appendStateIgnore, rf.curTerm(), ConflictInfo{}}
		return
	}

	if debug && len(args.Entries) == 0 {
		log.Printf("%v --%v--> %v, heartbeat reach, time:%v", args.LeaderId, args.Term, rf.me, atomic.LoadInt64(&rf.curTime))
	}

	rf.recordReceive()

	isTermChanged := rf.increaseTerm(args.Term)

	// append entry at specific location
	state, newN, conflict := rf.log.appendAt(args.PreEntry, args.Entries)
	if state == appendStateSucc {
		ok := rf.log.advanceCommitedLock(min(args.CommitedIndex, args.PreEntry.Index+len(args.Entries)))
		if debug {
			log.Printf("me=%v, append %v entry(s) at %v, new=%v", rf.me, len(args.Entries), args.PreEntry.getIndex()+1, newN)
			if ok {
				log.Printf("me=%v, set commited=%v", rf.me, min(args.CommitedIndex, args.PreEntry.Index+len(args.Entries)))
			}
		}
	}

	if isTermChanged || newN > 0 {
		rf.persist()
	}

	(*reply) = AppendEntryReply{state, rf.curTerm(), conflict}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// persist before return response to server!
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.curTerm())
	e.Encode(rf.voteFor)

	rf.log.lock()
	defer rf.log.unlock()
	e.Encode(len(rf.log.entries))
	e.Encode(rf.log.entries)
	e.Encode(rf.log.snapshot.Last)
	// e.Encode(len(rf.log.snapshot.Data))
	// e.Encode(rf.log.snapshot.Data)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot.Data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshotData []byte) {
	rf.lock()
	rf.log.lock()
	defer rf.log.unlock()
	defer rf.unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var voteFor int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil {

		log.Fatalf("readPersist: error decode")
	} else {
		rf.setTerm(term)
		rf.voteForLeader(voteFor)
	}

	// get entries
	var entriesLen int
	if d.Decode(&entriesLen) != nil {
		log.Fatalf("decode entriesLen failed")
	}
	entries := make([]logEntry, 0, entriesLen)
	if d.Decode(&entries) != nil {
		log.Fatalf("readPersist: decode entry failed")
	}
	rf.log.entries = entries
	// get snapshot
	var last entryDescriptor
	if d.Decode(&last) != nil {
		panic("readPersist: decode last or dataLen failed")
	}

	rf.log.replaceSnapshot(last, snapshotData)

	// FIXME: necessary here?
	// rf.machine.reset(last, snapshotData)
}

type SnapshotArgs struct {
	LeaderId   int // leader's id
	LeaderTerm int // leader's term
	// LastIncluded entryDescriptor // snapshot's last entry
	Offset       int // not use
	SnapshotLast entryDescriptor
	SnapshotData []byte
	Done         bool // not use
}

type SnapshotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if index <= 0 || index > rf.log.getMaxEntryIndexLock() {
		log.Fatalf("snapshot: invalid index %v", index)
	}

	if index < rf.log.getSnapshotLastLock().Index {
		panic("Snapshot: invalid snapshot, last index of new snapshot smaller than old's, may cause entries lost")
	}

	if index > rf.log.getMaxCommitedIndex() {
		panic("Snapshot: invalid snapshot, index bigger than max commited index")
	}

	last, _ := rf.log.getED(index)
	rf.log.replaceSnapshotLock(last, snapshot)

	err := rf.log.removeBeforeAnd(index)
	if err != nil {
		log.Fatalf("snapshot: why remove failed, %v", err.Error())
	}

	if debug {
		log.Printf("me=%v, snapshot, last=%v", rf.me, last)
	}
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.lock()
	defer rf.unlock()

	if args.LeaderTerm < rf.curTerm() {
		(*reply) = SnapshotReply{rf.term}
		return
	}

	if debug {
		log.Printf("%v --[%v]--> %v, InstallSnapshot, newLast=%v, oldLast:%v", args.LeaderId, args.LeaderTerm, rf.me, args.SnapshotLast, rf.log.getSnapshotLastLock())
	}

	rf.increaseTerm(args.LeaderTerm)

	rf.log.replaceSnapshotLock(args.SnapshotLast, args.SnapshotData)

	if rf.log.isIndexInLog(args.SnapshotLast.Index) {
		rf.log.removeBeforeAnd(args.SnapshotLast.Index)
	} else {
		rf.log.clearLog()
	}

	rf.persist()

	// apply snopshot to stat machine
	(*reply) = SnapshotReply{rf.curTerm()}
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateId int
	Term        int // term of candidate
	Latest      entryDescriptor
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	VoteGranted bool
	Term        int // term of server which sends reply
}

// lock raft outside
func (rf *Raft) voteForLeader(id int) {
	rf.voteFor = id
}

func (rf *Raft) voteForWho() int {
	return rf.voteFor
}

func (rf *Raft) resetVoteFor() {
	rf.voteFor = voteForNone
}

func (rf *Raft) voteForPeer(args *RequestVoteArgs) bool {
	// election restrict. To ensure that new leader have all commited log entry.
	latest := rf.log.getLatestED()
	if latest.Term > args.Latest.Term || (latest.Term == args.Latest.Term && latest.Index > args.Latest.Index) {
		if debug {
			log.Printf("%v --[%v]--> %v, reject vote, latest of current server are newer, cur:%v, peer:%v", rf.me, args.Term, args.CandidateId, latest, args.Latest)
		}

		return false
	}

	if rf.voteForWho() != voteForNone && rf.voteForWho() != args.CandidateId {
		if debug {
			log.Printf("%v --[%v]--> %v, reject vote, current server had vote for %v", rf.me, args.Term, args.CandidateId, rf.voteFor)
		}
		return false
	}

	return true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unlock()

	old := rf.curTerm()

	// ignore lower term
	if args.Term < rf.curTerm() {
		*reply = RequestVoteReply{false, old}
		return
	}

	isTermChanged := rf.increaseTerm(args.Term)

	if !rf.voteForPeer(args) {
		*reply = RequestVoteReply{false, old}
		if isTermChanged {
			rf.persist()
		}
		return
	}

	rf.recordReceive()

	rf.voteForLeader(args.CandidateId)

	rf.persist()

	*reply = RequestVoteReply{true, old}
}

func (rf *Raft) increaseTerm(term int) bool {
	if rf.curTerm() < term {
		rf.transToFollower(term)
		return true
	}

	return false
}

func (rf *Raft) increaseTermLock(term int) bool {
	rf.lock()
	defer rf.unlock()

	return rf.increaseTerm(term)
}

func (rf *Raft) sendHeartbeatTo(id int, args *AppendEntryArgs) {
	reply := AppendEntryReply{}

	if debug {
		log.Printf("%v--[%v]-->%v, send heartbeat, at %v", rf.me, args.Term, id, atomic.LoadInt64(&rf.curTime))
	}

	if ok := rf.peers[id].Call("Raft.AppendEntries", args, &reply); !ok {
		return
	}

	rf.increaseTermLock(reply.Term)
}

// loop to copy entry to server(id) until new term
func (rf *Raft) sendEntriesTo(server int, args *AppendEntryArgs) {
	reply := AppendEntryReply{}
	if debug {
		log.Printf("%v --[%v]--> %v, send entry with index %v, pre=[%v], entryLen=%v, curTime:%v", rf.me, args.Term, server, args.PreEntry.Index+1, args.PreEntry, len(args.Entries), atomic.LoadInt64(&rf.curTime))
	}

	thisTime := atomic.LoadInt64(&rf.curTime)

	if ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply); !ok {
		if debug {
			log.Printf("%v --[%v]--> %v, overtime, pre=[%v], entryLen=%v, time=%v", rf.me, args.Term, server, args.PreEntry, len(args.Entries), atomic.LoadInt64(&rf.curTime))
		}
		return
	}

	if isTermChanged := rf.increaseTermLock(reply.Term); isTermChanged {
		if debug {
			log.Printf("%v --[%v]--> %v, find higher term, pre=[%v], entryLen=%v, time=%v", rf.me, args.Term, server, args.PreEntry, len(args.Entries), atomic.LoadInt64(&rf.curTime))
		}
		return
	}

	nextIndex, matchIndex := rf.handleAppendEntriesReply(server, args, &reply)

	rf.updateMatchNextLock(server, thisTime, nextIndex, matchIndex)
}

func (rf *Raft) handleAppendEntriesReply(id int, args *AppendEntryArgs, reply *AppendEntryReply) (int, int) {
	// get index of next log entry need to be sended.
	switch reply.State {
	case appendStateSucc:
		if debug {
			log.Printf("%v --[%v]--> %v, append successful", rf.me, args.Term, id)
		}
		return args.PreEntry.Index + 1 + len(args.Entries), args.PreEntry.Index + len(args.Entries)
	case appendStatePrevNotExist:

		if debug {
			log.Printf("%v --[%v]--> %v, previous entry is not exist", rf.me, args.Term, id)
		}
		// follower log entry with preIndex is not exist, send log entry with index=conflict.LogLen
		return reply.Conflict.LogLen, 0
	case appendStatePrevConflict:
		if debug {
			log.Printf("%v --[%v]--> %v, previous entry conflict", rf.me, args.Term, id)
		}
		// suppose that log entries in current server whose index in [b, e) have same term which is conflict.Term
		// choose min(b, conflict.MinIndex) as nextSendIndex if b != e, else choose conflict.MinIndex
		b, _ := rf.log.equalLock(reply.Conflict.Term, args.PreEntry.Index)
		if b == -1 || reply.Conflict.MinIndex < b {
			return reply.Conflict.MinIndex, 0
		}

		return b, 0
	case appendStateIgnore:
		if debug {
			log.Printf("%v --[%v]--> %v, append message is ignore", rf.me, args.Term, id)
		}
		return 0, 0
	default:
		log.Fatalf("invalid appendState:%v", reply.State)
	}
	return 0, 0
}

func (rf *Raft) sendHeartBeat(thisTerm int) {
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			continue
		}
		go func(id int) {
			args := AppendEntryArgs{rf.me, []logEntry{}, rf.log.getLatestED(), thisTerm, rf.log.getMaxCommitedIndex()}
			rf.sendHeartbeatTo(id, &args)
		}(id)
	}
	rf.recordHeartbeat()
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs) {
	reply := SnapshotReply{}

	thisTime := atomic.LoadInt64(&rf.curTime)

	if ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply); !ok {
		return
	}

	if isTermChanged := rf.increaseTermLock(reply.Term); isTermChanged {
		return
	}

	rf.updateMatchNextLock(server, thisTime, args.SnapshotLast.Index+1, 0)
}

func (rf *Raft) generateAppendEntryArg(nextSend int, thisTerm int) AppendEntryArgs {
	latest := rf.log.getLatestED()

	if nextSend > latest.Index+1 {
		panic(fmt.Sprintf("generateAppendEntryArg: nextSend %v should smaller than %v", nextSend, latest.Index+2))
	}

	if nextSend == latest.Index+1 {
		return AppendEntryArgs{rf.me, []logEntry{}, latest, thisTerm, rf.log.getMaxCommitedIndex()}
	}

	pre, entries := rf.log.entriesAfterAnd(nextSend)

	return AppendEntryArgs{rf.me, entries, pre, thisTerm, rf.log.getMaxCommitedIndex()}
}

// copy log entry to all other server
func (rf *Raft) sendEntries(thisTerm int) {
	if rf.curTermLock() != thisTerm || rf.curStateLock() != Leader {
		return
	}

	// Send entries asynchronous
	// Because sending entries may be delaied by unknown reason, but heartbeat can be received normal.
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		go func(server int) {
			nextSend := rf.sendRecord.getNext(server)
			if nextSend == 0 {
				panic("sendEntries: why nextSend is zero?")
			}

			if rf.log.isIndexInSnapshotLock(nextSend) {
				last, data := rf.log.snapshot.clone()
				args := SnapshotArgs{rf.me, thisTerm, 0, last, data, true}
				rf.sendInstallSnapshot(server, &args)
				return
			}

			args := rf.generateAppendEntryArg(nextSend, thisTerm)
			rf.sendEntriesTo(server, &args)
		}(server)
	}

	rf.recordSendEntries()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.lock()
	defer rf.unlock()

	if rf.curState() != Leader {
		return -1, -1, false
	}

	index := rf.log.append(rf.curTerm(), command)

	if debug {
		log.Printf("me=%v, append new entry, entry=%v", rf.me, logEntry{index, rf.term, command})
	}

	// only modify matchIndex of current sever here
	rf.updateMatchNext(rf.me, atomic.LoadInt64(&rf.curTime), 0, index)

	rf.persist()

	return index, rf.curTerm(), true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) handleVoteReplies(thisTerm int, replyCh chan RequestVoteReply) int {
	once := 0
	agrees := 1

	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-replyCh

		if reply.VoteGranted {
			agrees++
		}

		if rf.increaseTermLock(reply.Term) {
			continue
		}

		if agrees == rf.agreeMin() && once == 0 {
			rf.becomeLeader(thisTerm)
			once = 1
		}
	}

	if debug {
		log.Printf("me=%v, get %v votes at term %v", rf.me, agrees, thisTerm)
	}
	return agrees
}

// send request vote to each server, and fetch result until receive enough tickets either agree or reject
// first return value is true if receive enough agree, false if receive enough reject
// second return value is the max term of all the response
func (rf *Raft) fetchVotes(thisTerm int, latest entryDescriptor, replyCh chan RequestVoteReply) {
	// get votes from other server
	for i := 0; i < len(rf.peers); i++ {
		// send heartbeat to other server
		if i == rf.me {
			continue
		}

		go func(id int) {
			args := RequestVoteArgs{rf.me, thisTerm, latest}
			reply := RequestVoteReply{}
			// labrpc.go may block here rather than timeout,
			if ok := rf.peers[id].Call("Raft.RequestVote", &args, &reply); ok {
				replyCh <- reply
			} else {
				reply.Term = 0
				reply.VoteGranted = false
				replyCh <- reply
			}
		}(i)
	}
}

func (rf *Raft) agreeMin() int {
	// len(rf.peers)  agreeMin()
	//            3           2
	//            5           3
	//            6           4
	return (len(rf.peers) + 1 + 1) / 2
}

func (rf *Raft) becomeLeader(thisTerm int) bool {
	// transform state of server according to vote result
	rf.lock()
	defer rf.unlock()

	if thisTerm != rf.curTerm() {
		if debug {
			log.Printf("me=%v, term %v election done, lose, term changed ", rf.me, thisTerm)
		}
		return false
	}

	if debug {
		log.Printf("me=%v, become leader, term=%v", rf.me, thisTerm)
	}

	rf.transToLeader()
	return true
}

// start election to be the leader of term
func (rf *Raft) startElection() {
	// FIXME:some server could not be elected, but start election frequently.
	// other valid candidata may elect failed because its term being changed
	// 1. stop/defer election if receive major latest entry reject

	thisTerm := rf.transToCandidate()
	if thisTerm == -1 {
		return
	}

	replyCh := make(chan RequestVoteReply, len(rf.peers)-1)
	rf.fetchVotes(thisTerm, rf.log.getLatestED(), replyCh)
	rf.handleVoteReplies(thisTerm, replyCh)
}

func (rf *Raft) recordHeartbeat() {
	atomic.StoreInt64(&rf.lastSendHeartbeat, atomic.LoadInt64(&rf.curTime))
}

func (rf *Raft) recordSendEntries() {
	atomic.StoreInt64(&rf.lastSendEntries, atomic.LoadInt64(&rf.curTime))
}

func (rf *Raft) recordReceive() {
	rf.resetElectTimer(atomic.LoadInt64(&rf.curTime))
}

func (rf *Raft) resetElectTimer(t int64) {
	ms := electionMin + (rand.Int63() % (electionMax - electionMin))
	atomic.StoreInt64(&rf.nextElect, ms+t)
}

func (rf *Raft) sendHeartbeatNextTick() {
	atomic.StoreInt64(&rf.lastSendHeartbeat, 0)
}

func (rf *Raft) isSendHeartbeat() bool {
	return atomic.LoadInt64(&rf.lastSendHeartbeat)+heartbeatInterval <= atomic.LoadInt64(&rf.curTime)
}

func (rf *Raft) isSendEntries() bool {
	return atomic.LoadInt64(&rf.lastSendEntries)+sendEntriesInterval <= atomic.LoadInt64(&rf.curTime)
}

func (rf *Raft) transToFollower(newTerm int) {
	rf.setTerm(newTerm)
	rf.setState(Follower)
	rf.resetVoteFor()
}

func (rf *Raft) transToCandidate() int {
	rf.lock()
	defer rf.unlock()
	if rf.curState() == Leader {
		return -1
	}

	rf.setState(Candidate)

	rf.setTerm(rf.curTerm() + 1)

	rf.voteForLeader(rf.me)

	rf.resetElectTimer(atomic.LoadInt64(&rf.curTime))

	rf.persist()

	if debug {
		log.Printf("me=%v, term %v election start, time=%v", rf.me, rf.curTerm(), atomic.LoadInt64(&rf.curTime))
	}

	return rf.curTerm()
}

func (rf *Raft) transToLeader() {
	rf.setState(Leader)
	latestIndex := rf.log.getLatestED().Index
	rf.sendRecord.initialize(latestIndex+1, atomic.LoadInt64(&rf.curTime))

	rf.sendHeartbeatNextTick()
}

func (rf *Raft) nextTick() {
	time.Sleep(time.Duration(tickMilisec) * time.Millisecond)
	atomic.AddInt64(&rf.curTime, tickMilisec)
}

func (rf *Raft) isStartElection() bool {
	return atomic.LoadInt64(&rf.curTime) >= atomic.LoadInt64(&rf.nextElect)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		switch rf.curState() {
		case Follower:
			if rf.isStartElection() {
				go rf.startElection()
			}
		case Candidate:
			if rf.isStartElection() {
				go rf.startElection()
			}
		case Leader:
			if rf.isSendHeartbeat() {
				go rf.sendHeartBeat(rf.curTerm())
			}
			if rf.isSendEntries() {
				go rf.sendEntries(rf.curTerm())
			}
			go rf.updateCommited(rf.curTerm())
		}
		rf.mu.Unlock()

		rf.nextTick()
	}
}

func (rf *Raft) applyLogEntry() {
	// apply log whose index in (maxApplyIndex, maxCommitedIndex] periodically
	for !rf.killed() {
		maxApplied := rf.machine.getApplied().Index
		maxCommitedIndex := rf.log.getMaxCommitedIndex()
		applyNums := maxCommitedIndex - maxApplied
		if applyNums < 0 {
			log.Fatalf("applyLogEntry: why applied uncommited log entry?")
		}

		if applyNums == 0 {
			time.Sleep(time.Duration(applyInterval) * time.Millisecond)
			continue
		}

		if rf.log.isIndexInSnapshotLock(maxApplied + 1) {
			// reset state machine if log entry is in snapshot
			last, data := rf.log.snapshot.clone()
			rf.machine.reset(last, data)
			if debug {
				log.Printf("me=%v, reset state machine, last=%v", rf.me, last)
			}
			continue
		}

		if !rf.log.isIndexInLog(maxApplied + 1) {
			panic("applyLogEntry: why commited entry not in log?")
		}

		// entries may not exist
		_, entries := rf.log.entriesAfterAnd(maxApplied+1, applyNums)

		rf.machine.applyLogEntries(entries)
		if debug {
			log.Printf("me=%v, apply entries, entries=%v", rf.me, entries)
		}
	}
}

//-------------------------------- Raft -----------------------------------

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.construct(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	go rf.ticker()

	go rf.applyLogEntry()

	return rf
}
