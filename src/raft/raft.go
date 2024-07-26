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
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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

func (rf *Raft) append(term int, command interface{}) int {
	// lock raft outside
	rf.log.lock()
	defer rf.log.unlock()

	newIndex := len(rf.log.entries)

	if newIndex != rf.log.entries[len(rf.log.entries)-1].Index+1 {
		log.Fatalf("index of log entry should increase monotonically")
	}

	rf.log.entries = append(rf.log.entries, logEntry{newIndex, term, command})

	// only modify matchIndex of current sever here
	rf.sendEntryInfo.updateMatch(rf.me, newIndex)

	rf.persist()

	if debug {
		log.Printf("me=%v, append [i:%v t:%v c:%v]", rf.me, newIndex, term, command)
	}

	return newIndex
}

func (rl *raftLog) equalLock(term int, last int) (int, int) {
	rl.lock()
	defer rl.unlock()

	return rl.equal(term, last)
}

// lock outside
// get range of term in [rl.startAt, last]
// if index last not exist, return -1, -1
func (rl *raftLog) equal(term int, last int) (int, int) {
	te := -1
	tb := -1

	for i := last; rl.isIndexExist(i) && rl.entries[rl.slotId(i)].Term >= term; i-- {
		if rl.entries[rl.slotId(i)].Term > term {
			continue
		}
		// t == term
		if te == -1 {
			te = i + 1
		}

		tb = i
	}

	return tb, te
}

// insert e after entry(index, term)
// return false if entry(index, term) is not exist
func (rf *Raft) appendAt(pre entryDescriptor, entries []logEntry) (AppendStateType, int, ConflictInfo) {
	// lock raft outside

	rf.log.lock()
	defer rf.log.unlock()

	if pre.Index < 0 {
		log.Fatalf("appendAt: pIndex should greater or equal than 0")
	}

	if !rf.log.isIndexExist(pre.Index) {
		return appendStatePrevNotExist, 0, ConflictInfo{-1, -1, len(rf.log.entries)}
	}

	if !rf.log.isEntryExist(&pre) {
		// previous log entry is conflict with entry(pIndex, pTerm)
		term := rf.log.entries[rf.log.slotId(pre.Index)].Term
		minIndex, _ := rf.log.equal(term, pre.Index)
		return appendStatePrevConflict, 0, ConflictInfo{term, minIndex, len(rf.log.entries)}
	}

	if len(entries) == 0 {
		// heartbeat
		return appendStateSucc, 0, ConflictInfo{-1, -1, len(rf.log.entries)}
	}

	// if there conflict entry, delete it and follows, then append new entry.
	// entries:       b-1 [b, l]              [fD, l]
	// log.entries:   pre [l_b, l_l]          [fD, l_l]
	//                fD <= l, fD <= l_l
	firstDiffIndex := rf.log.getDiffSuffix(entries)

	// no new entry to add
	if rf.log.entries[len(rf.log.entries)-1].Index >= len(entries) && firstDiffIndex > entries[len(entries)-1].Index {
		return appendStateSucc, 0, ConflictInfo{-1, -1, len(rf.log.entries)}
	}

	rf.log.entries = append(rf.log.entries[:rf.log.slotId(firstDiffIndex)], entries[firstDiffIndex-entries[0].Index:]...)

	if debug {
		log.Printf("me=%v, add entries, len=%v, entry=[%v]", rf.me, entries[len(entries)-1].Index-firstDiffIndex+1, entries[firstDiffIndex-entries[0].Index:])
		rf.log.checkConsistency(0)
	}

	return appendStateSucc, entries[len(entries)-1].Index - firstDiffIndex + 1, ConflictInfo{-1, -1, len(rf.log.entries)}
}

type sendEntryInfo struct {
	mu           sync.RWMutex
	nextIndex    []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex   []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	lastModifies []int64
}

func (sei *sendEntryInfo) getNext(id int) int {
	sei.mu.RLock()
	defer sei.mu.RUnlock()

	return sei.nextIndex[id]
}

func (sei *sendEntryInfo) construct(len int) {
	sei.nextIndex = make([]int, len)
	sei.matchIndex = make([]int, len)
	sei.lastModifies = make([]int64, len)
}

func (sei *sendEntryInfo) initialize(index int, t int64) {
	sei.mu.Lock()
	defer sei.mu.Unlock()

	for i := 0; i < len(sei.matchIndex); i++ {
		sei.nextIndex[i] = index
		sei.matchIndex[i] = 0
		sei.lastModifies[i] = t
	}
}

func (sei *sendEntryInfo) copyMatchIndex() []int {
	sei.mu.RLock()
	defer sei.mu.RUnlock()

	matchIndex := make([]int, len(sei.matchIndex))
	copy(matchIndex, sei.matchIndex)
	return matchIndex
}

func (sei *sendEntryInfo) updateMatch(id int, index int) {
	sei.matchIndex[id] = index
}

func (rf *Raft) updateMatchNext(id int, thisTime int64, nextIndex int, matchIndex int) {
	rf.sendEntryInfo.mu.Lock()
	defer rf.sendEntryInfo.mu.Unlock()
	// do not allow older Appendentries update next
	if thisTime <= rf.sendEntryInfo.lastModifies[id] {
		return
	}

	rf.sendEntryInfo.lastModifies[id] = thisTime

	if nextIndex != 0 {
		rf.sendEntryInfo.nextIndex[id] = nextIndex
	}

	if matchIndex != 0 {
		rf.sendEntryInfo.matchIndex[id] = matchIndex
	}

	if debug {
		log.Printf("%v ----> %v, next=%v, match=%v", rf.me, id, rf.sendEntryInfo.nextIndex[id], rf.sendEntryInfo.matchIndex[id])
	}

}

//-------------------------------- Raft -------------------------------------

// A Go object implementing a single Raft peer.
type Raft struct {
	peers         []*labrpc.ClientEnd // RPC end points of all peers
	persister     *Persister          // Object to hold this peer's persisted state
	me            int                 // this peer's index into peers[]
	dead          int32               // set by Kill()
	mu            sync.Mutex          // Lock to protect shared access to this peer's state
	sendEntryInfo sendEntryInfo
	// nextIndex       []int               // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	// matchIndex      []int               // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	// lastUpdateIndex int64
	term            int           // term of current server
	voteFor         int           // vote for which server this term
	state           RaftStateType // state of current server
	log             raftLog       // content of log entry
	machine         stateMachine  // state machine
	curTimeMiliSecs int64
	nextElect       int64
	lastSendEntries int64
	lastHeartbeat   int64
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

	if thisTerm != rf.curTerm() {
		return
	}

	l := len(rf.peers)

	matchIndex := rf.sendEntryInfo.copyMatchIndex()

	sort.Ints(matchIndex)
	// update commited log entry
	if ed, _ := rf.log.getED(matchIndex[l-rf.agreeMin()]); ed.Term == rf.curTerm() {
		ok := rf.log.advanceCommited(ed.Index)
		if ok && debug {
			log.Printf("me=%v, term %v commied %v", rf.me, rf.curTerm(), ed)
		}
	}
}

func (rf *Raft) String() string {
	return fmt.Sprintf("me:%v, term:%v, state:%v", rf.me, rf.curTerm(), rf.state)
}

func (rf *Raft) construct(applyCh chan ApplyMsg) {
	rf.sendEntryInfo.construct(len(rf.peers))
	rf.setTerm(0)
	rf.resetVoteFor()
	rf.setState(Follower)
	rf.curTimeMiliSecs = 0
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
		log.Printf("%v --%v--> %v, heartbeat reach, time:%v", args.LeaderId, args.Term, rf.me, atomic.LoadInt64(&rf.curTimeMiliSecs))
	}

	rf.recordReceive()

	isTermChanged := rf.lowerThan(args.Term)

	// append entry at specific location
	state, newN, conflict := rf.appendAt(args.PreEntry, args.Entries)
	if state == appendStateSucc {
		// commited = min(commitedIndex, index of new entry(not the last entry...))
		ok := rf.log.advanceCommited(min(args.CommitedIndex, args.PreEntry.Index+len(args.Entries)))
		if debug && ok {
			log.Printf("me=%v, set commited=%v", rf.me, min(args.CommitedIndex, args.PreEntry.Index+len(args.Entries)))
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

	e.Encode(len(rf.log.entries))
	e.Encode(rf.log.entries)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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

	var entriesLen int
	// var entriesLen int
	if d.Decode(&entriesLen) != nil {
		log.Fatalf("decode entriesLen failed")
	}

	entries := make([]logEntry, 0, entriesLen)
	if d.Decode(&entries) != nil {
		log.Fatalf("readPersist: decode entry failed")
	}
	rf.log.entries = entries

	if debug {
		log.Printf("me=%v, load entries, entris=%v", rf.me, rf.log.entries)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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

	// do not reset election here, for some invalid elect may defer valid election

	isHigher := rf.lowerThan(args.Term)

	if !rf.voteForPeer(args) {
		*reply = RequestVoteReply{false, old}
		if isHigher {
			rf.persist()
		}
		return
	}

	rf.recordReceive()

	rf.voteForLeader(args.CandidateId)

	rf.persist()

	*reply = RequestVoteReply{true, old}
}

func (rf *Raft) lowerThan(term int) bool {
	if rf.curTerm() < term {
		rf.transToFollower(term)
		return true
	}

	return false
}

func (rf *Raft) lowerThanLock(term int) bool {
	rf.lock()
	defer rf.unlock()

	return rf.lowerThan(term)
}

func (rf *Raft) sendHeartbeatTo(id int, args *AppendEntryArgs) {
	reply := AppendEntryReply{}

	if debug {
		log.Printf("%v--[%v]-->%v, send heartbeat, at %v", rf.me, args.Term, id, atomic.LoadInt64(&rf.curTimeMiliSecs))
	}

	if ok := rf.peers[id].Call("Raft.AppendEntries", args, &reply); !ok {
		return
	}

	rf.lowerThanLock(reply.Term)
}

// loop to copy entry to server(id) until new term
func (rf *Raft) sendEntriesTo(id int, args *AppendEntryArgs) {

	// args := AppendEntryArgs{rf.me, entries, *pre, thisTerm, rf.log.getMaxCommitedIndex()}
	reply := AppendEntryReply{}
	if debug {
		log.Printf("%v --[%v]--> %v, send entry with index %v, pre=[%v], entryLen=%v, curTime:%v", rf.me, args.Term, id, args.PreEntry.Index+1, args.PreEntry, len(args.Entries), atomic.LoadInt64(&rf.curTimeMiliSecs))
	}

	thisTime := atomic.LoadInt64(&rf.curTimeMiliSecs)

	if ok := rf.peers[id].Call("Raft.AppendEntries", args, &reply); !ok {
		if debug {
			log.Printf("%v --[%v]--> %v, overtime, pre=[%v], entryLen=%v, time=%v", rf.me, args.Term, id, args.PreEntry, len(args.Entries), atomic.LoadInt64(&rf.curTimeMiliSecs))
		}
		return
	}

	if isHigher := rf.lowerThanLock(reply.Term); isHigher {
		if debug {
			log.Printf("%v --[%v]--> %v, find higher term, pre=[%v], entryLen=%v, time=%v", rf.me, args.Term, id, args.PreEntry, len(args.Entries), atomic.LoadInt64(&rf.curTimeMiliSecs))
		}
		return
	}

	nextIndex, matchIndex := rf.handleAppendEntriesReply(id, args, &reply)

	rf.updateMatchNext(id, thisTime, nextIndex, matchIndex)
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
			// rf.recordSendEntries()

			args := AppendEntryArgs{rf.me, []logEntry{}, rf.log.getLatestED(), thisTerm, rf.log.getMaxCommitedIndex()}
			rf.sendHeartbeatTo(id, &args)
		}(id)
	}
	rf.recordHeartbeat()
}

// copy log entry to all other server
func (rf *Raft) sendEntries(thisTerm int) {
	if rf.curTermLock() != thisTerm || rf.curStateLock() != Leader {
		return
	}

	// Send entries asynchronous
	// Because sending entries may be delaied by unknown reason, but heartbeat can be received normal.
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			continue
		}

		go func(id int) {
			pre, entries, err := rf.log.entriesAfter(rf.sendEntryInfo.getNext(id) - 1)
			if err != nil {
				log.Fatalf("sendEntries: pre entry should exist")
			}

			args := AppendEntryArgs{rf.me, entries, pre, thisTerm, rf.log.getMaxCommitedIndex()}

			// resend if this call overtime
			rf.sendEntriesTo(id, &args)
		}(id)
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

	index := rf.append(rf.curTerm(), command)

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

		if rf.lowerThanLock(reply.Term) {
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

// func (rf *Raft) rejectMax() int {
// 	// len(rf.peers)   rejectMax()
// 	//            5             3
// 	//            6             3
// 	return (len(rf.peers) + 1) / 2
// }

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
	atomic.StoreInt64(&rf.lastHeartbeat, atomic.LoadInt64(&rf.curTimeMiliSecs))
}

func (rf *Raft) recordSendEntries() {
	atomic.StoreInt64(&rf.lastSendEntries, atomic.LoadInt64(&rf.curTimeMiliSecs))
}

func (rf *Raft) recordReceive() {
	rf.resetElectTimer(atomic.LoadInt64(&rf.curTimeMiliSecs))
}

func (rf *Raft) resetElectTimer(t int64) {
	ms := electionMin + (rand.Int63() % (electionMax - electionMin))
	atomic.StoreInt64(&rf.nextElect, ms+t)
}

func (rf *Raft) sendHeartbeatNextTick() {
	atomic.StoreInt64(&rf.lastHeartbeat, 0)
}

func (rf *Raft) isSendHeartbeat() bool {
	return atomic.LoadInt64(&rf.lastHeartbeat)+heartbeatInterval <= atomic.LoadInt64(&rf.curTimeMiliSecs)
}

func (rf *Raft) isSendEntries() bool {
	return atomic.LoadInt64(&rf.lastSendEntries)+sendEntriesInterval <= atomic.LoadInt64(&rf.curTimeMiliSecs)
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

	rf.resetElectTimer(atomic.LoadInt64(&rf.curTimeMiliSecs))

	rf.persist()

	if debug {
		log.Printf("me=%v, term %v election start, time=%v", rf.me, rf.curTerm(), atomic.LoadInt64(&rf.curTimeMiliSecs))
	}

	return rf.curTerm()
}

func (rf *Raft) transToLeader() {
	rf.setState(Leader)
	latestIndex := rf.log.getLatestED().Index
	rf.sendEntryInfo.initialize(latestIndex+1, atomic.LoadInt64(&rf.curTimeMiliSecs))

	rf.sendHeartbeatNextTick()
}

func (rf *Raft) nextTick() {
	time.Sleep(time.Duration(tickMilisec) * time.Millisecond)
	atomic.AddInt64(&rf.curTimeMiliSecs, tickMilisec)
}

func (rf *Raft) isStartElection() bool {
	return atomic.LoadInt64(&rf.curTimeMiliSecs) >= atomic.LoadInt64(&rf.nextElect)
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
			go rf.updateCommited(rf.curTerm())
			if rf.isSendHeartbeat() {
				go rf.sendHeartBeat(rf.curTerm())
			}
			if rf.isSendEntries() {
				go rf.sendEntries(rf.curTerm())
			}
		}
		rf.mu.Unlock()

		rf.nextTick()
	}
}

func (rf *Raft) applyLogEntry() {

	// apply log whose index in (maxApplyIndex, maxCommitedIndex] periodically
	for !rf.killed() {
		maxApplied := rf.machine.getApplied().Index
		nums := rf.log.getMaxCommitedIndex() - maxApplied
		if nums <= 0 {
			time.Sleep(time.Duration(applyInterval) * time.Millisecond)
			continue
		}
		_, entries, _ := rf.log.entriesAfter(maxApplied, nums)
		rf.machine.applyLogEntries(entries)

		if debug {
			log.Printf("me=%v, apply %v", rf.me, entries)
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
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	go rf.applyLogEntry()

	return rf
}
