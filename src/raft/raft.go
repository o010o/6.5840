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
	"errors"
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
	Follower                RaftStateType   = 0
	Candidate               RaftStateType   = 1
	Leader                  RaftStateType   = 2
	appendStateSucc         AppendStateType = 0 // append successfully
	appendStateIgnore       AppendStateType = 1 // append message is ignore because of term
	appendStatePrevNotExist AppendStateType = 2 // append failed because previous entry is not exist
	appendStatePrevConflict AppendStateType = 3
	appendStateRepeated     AppendStateType = 4
)

//---------------------------- stateMachine ------------------------------

type stateMachine struct {
	applyCh chan ApplyMsg
	mu      sync.RWMutex
	applied entryDescriptor
}

func (m *stateMachine) construct(ch chan ApplyMsg) {
	m.applyCh = ch
	m.applied.Index = 0
	m.applied.Term = 0
}

func (m *stateMachine) getApplied() entryDescriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.applied
}

func (m *stateMachine) applyLogEntry(index int, e logEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if index != m.applied.Index+1 {
		log.Fatalf("invalid apply sequence")
	}

	m.applyCh <- ApplyMsg{true, e.Command, index, false, []byte{}, 0, 0}
	m.applied.Term = e.Term
	m.applied.Index = index
}

//---------------------------- stateMachine ------------------------------

//----------------------------- logEntry --------------------------------

type logEntry struct {
	Term    int
	Command interface{}
}

//----------------------------- logEntry --------------------------------

//----------------------------- raftLog --------------------------------

type raftLog struct {
	mu sync.RWMutex
	// apply may block because of net, so we have to try apply log entry until success. So just send log entry periodically.
	// cond             sync.Cond   // signal there are new commited entries
	entries          []logEntry // pointer to content of log entry
	startAt          int        // the index of first entry in entries
	maxCommitedIndex int        // biggest index which can apply to state machine safely.
	// latestCommited entryDescriptor // latest commited entry
}

func (rf *raftLog) lock() {
	rf.mu.Lock()
}

func (rf *raftLog) unlock() {
	rf.mu.Unlock()
}

func (rf *raftLog) rLock() {
	rf.mu.RLock()
}

func (rf *raftLog) rUnLock() {
	rf.mu.RUnlock()
}

func (rl *raftLog) advanceCommited(commited *entryDescriptor) bool {
	rl.lock()
	defer rl.unlock()

	if commited.Index <= rl.maxCommitedIndex {
		return false
	}

	if !rl.isEntryExist(commited) {
		return false
	}

	rl.maxCommitedIndex = commited.Index

	return true
}

func (rl *raftLog) getED(index int) entryDescriptor {
	rl.rLock()
	defer rl.rUnLock()

	if !rl.isIndexExist(index) {
		return entryDescriptor{}
	}

	return entryDescriptor{index, rl.entries[rl.slotId(index)].Term}
}

func (rl *raftLog) construct() {
	rl.entries = make([]logEntry, 1)
	rl.entries[0] = logEntry{0, nil}
	rl.startAt = 0
	rl.maxCommitedIndex = 0
}

func (rl *raftLog) getMaxCommitedED() entryDescriptor {
	rl.rLock()
	defer rl.rUnLock()

	return entryDescriptor{rl.maxCommitedIndex, rl.entries[rl.slotId(rl.maxCommitedIndex)].Term}
}

func (rl *raftLog) getLatestED() entryDescriptor {
	rl.rLock()
	defer rl.rUnLock()

	return entryDescriptor{len(rl.entries) - 1, rl.entries[len(rl.entries)-1].Term}
}

func (rl *raftLog) isIndexExist(index int) bool {
	if index < rl.startAt || index >= len(rl.entries)+rl.startAt {
		return false
	}

	return true
}

func (rl *raftLog) isEntryExist(entry *entryDescriptor) bool {
	if !rl.isIndexExist(entry.Index) {
		return false
	}

	return rl.entries[rl.slotId(entry.Index)].Term == entry.Term
}

func (rf *Raft) append(e *logEntry) int {
	// lock raft outside
	rf.log.lock()
	defer rf.log.unlock()

	newIndex := len(rf.log.entries)

	rf.log.entries = append(rf.log.entries, *e)

	// only modify matchIndex of current sever here
	atomic.StoreInt32(&rf.matchIndex[rf.me], int32(newIndex))

	// str := ""
	// str = fmt.Sprintf("me=%v, commited=%v, term=%v, entry=[", rf.me, rf.log.maxCommitedIndex, rf.term)
	// for i := 0; i < len(rf.log.entries); i++ {
	// 	str += fmt.Sprintf("%v ", rf.log.entries[i].Term)
	// }
	// str = str[:len(str)-1]
	// str += "]"
	// fmt.Printf("%v \n", str)

	// persist log entry before it can be seen
	rf.persist()

	return newIndex
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
func (rf *Raft) appendAt(pre entryDescriptor, entries []logEntry) (AppendStateType, ConflictInfo) {
	// lock raft outside
	rf.log.lock()
	defer rf.log.unlock()

	if pre.Index < 0 {
		log.Fatalf("appendAt: pIndex should greater or equal than 0")
	}

	if len(entries) == 0 {
		log.Fatalf("appendAt: empty entries")
	}

	if !rf.log.isIndexExist(pre.Index) {
		return appendStatePrevNotExist, ConflictInfo{-1, -1, len(rf.log.entries)}
	}

	if !rf.log.isEntryExist(&pre) {
		// log entry(pIndex, pTerm) may not exist or conflict with previous log entry
		if !rf.log.isIndexExist(pre.Index) {
			// previous log entry is not exist
			log.Fatalf("appendAt: index %v should exist here", pre.Index)
		}

		// previous log entry is conflict with entry(pIndex, pTerm)
		term := rf.log.entries[rf.log.slotId(pre.Index)].Term
		minIndex, _ := rf.log.equal(term, pre.Index)
		return appendStatePrevConflict, ConflictInfo{term, minIndex, len(rf.log.entries)}
	}

	// do not modify commited log
	if pre.Index >= rf.log.maxCommitedIndex {
		rf.log.entries = append(rf.log.entries[:pre.Index+1], entries...)
	} else {
		// find entry with index = maxCommitedIndex + 1
		// pre.Index + 1
		if pre.Index+len(entries) <= rf.log.maxCommitedIndex {
			return appendStateSucc, ConflictInfo{}
		}

		b := rf.log.maxCommitedIndex - pre.Index
		rf.log.entries = append(rf.log.entries[:rf.log.slotId(rf.log.maxCommitedIndex+1)], entries[b:]...)
	}

	// str := ""
	// str = fmt.Sprintf("me=%v, maxCommited=%v, term=%v, appendAt entry=[", rf.me, rf.log.maxCommitedIndex, rf.term)
	// for i := 0; i < len(rf.log.entries); i++ {
	// 	str += fmt.Sprintf("%v ", rf.log.entries[i].Term)
	// }
	// str = str[:len(str)-1]
	// str += "]"
	// fmt.Printf("%v\n", str)

	// persist log entry before it can be seen
	rf.persist()

	return appendStateSucc, ConflictInfo{}
}

// return slot id of index
func (rl *raftLog) slotId(index int) int {
	// used before lock!
	return index - rl.startAt
}

// return slice of entries, dont modify this slice
func (rl *raftLog) entriesAfter(index int, numsOpt ...int) (entryDescriptor, []logEntry, error) {
	rl.rLock()
	defer rl.rUnLock()

	if !rl.isIndexExist(index) {
		return entryDescriptor{-1, -1}, []logEntry{}, errors.New("invalid index")
	}

	if index >= len(rl.entries) {
		return entryDescriptor{-1, -1}, []logEntry{}, errors.New("no entries")
	}

	nums := -1
	if len(numsOpt) > 0 {
		nums = numsOpt[0]
	}

	if nums == -1 {
		// if term maintains
		// slice still can be used even there is new entry appends to log
		// if term end, and this server transform to follower, and the log was truncated.
		// slice returned can be accessed safely without segement, only the data we dont use would be changed
		return entryDescriptor{index, rl.entries[rl.slotId(index)].Term}, rl.entries[index+1:], nil
	}

	// fetch [index + 1, index + 1 + nums)
	e := index + 1 + nums
	if e > len(rl.entries) {
		log.Fatalf("no enough entry to return")
	}

	return entryDescriptor{index, rl.entries[rl.slotId(index)].Term}, rl.entries[rl.slotId(index+1):rl.slotId(e)], nil
}

//-------------------------------- raftLog ----------------------------------

//-------------------------------- Raft -------------------------------------

// A Go object implementing a single Raft peer.
type Raft struct {
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	mu         sync.Mutex          // Lock to protect shared access to this peer's state
	nextIndex  []int               // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int32             // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	term       int                 // term of current server
	voteFor    int                 // vote for which server this term
	state      RaftStateType       // state of current server
	isRecvHB   bool                // did this server receive heartbeat recently?
	log        raftLog             // content of log entry
	machine    stateMachine        // state machine
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) advanceCommited(thisTerm int) {
	rf.lock()
	defer rf.unlock()

	if rf.term != thisTerm {
		return
	}

	l := len(rf.peers)

	matchIndex := make([]int, l)
	for i := 0; i < l; i++ {
		matchIndex[i] = int(atomic.LoadInt32(&rf.matchIndex[i]))
	}

	sort.Ints(matchIndex)

	ed := rf.log.getED(matchIndex[l-rf.agreeMin()])
	if ed.Term == thisTerm {
		rf.log.advanceCommited(&ed)
	}
}

func (rf *Raft) String() string {
	return fmt.Sprintf("me:%v, term:%v, state:%v, isRecvHB:%v", rf.me, rf.term, rf.state, rf.isRecvHB)
}

func (rf *Raft) construct(applyCh chan ApplyMsg) {
	rf.term = 0
	rf.state = Follower
	rf.isRecvHB = true
	rf.resetVote()

	rf.log.construct()
	rf.machine.construct(applyCh)

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unlock()

	// Your code here (3A).
	return rf.term, rf.state == Leader
}

func (rf *Raft) GetRaftState() RaftStateType {
	rf.lock()
	defer rf.unlock()

	return rf.state
}

//-------------------------------- Raft -------------------------------------

func (rf *Raft) extendTerm() {
	// cause ticker not to call election this recycle
	rf.isRecvHB = true
}

func (rf *Raft) getTerm() int {
	rf.lock()
	defer rf.unlock()

	return rf.term
}

type entryDescriptor struct {
	Index int
	Term  int
}

type AppendEntryArgs struct {
	LeaderId int             // id of the leader who sends this message
	Entries  []logEntry      // Entries need to append into follower
	PreEntry entryDescriptor // append Entries after PreEntry
	Term     int             // term of server
	Commited entryDescriptor // latest entry Commited to state machine
}

func (aep AppendEntryArgs) String() string {
	return fmt.Sprintf("preEntry=[%v], entryLen=%v, thisTerm=%v, Commited=%v", aep.PreEntry, len(aep.Entries), aep.Term, aep.Commited)
}

func (aep *AppendEntryArgs) isHeartBeat() bool {
	return len(aep.Entries) == 0
}

type ConflictInfo struct {
	Term     int // term of conflict log entry if append failed
	MinIndex int // min index of log entry with term = XTerm if append failed
	LogLen   int // len of peer server log
}

type AppendEntryReply struct {
	State AppendStateType // 0:succ, 1:lower term, 2:
	Term  int             // term of peer server
	// Conflict interface{}
	Conflict ConflictInfo // labgob will abort if using interface{} here for unknown reason
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.lock()
	defer rf.unlock()

	if rf.term > args.Term {
		// caller find higher term
		(*reply) = AppendEntryReply{appendStateIgnore, rf.term, ConflictInfo{}}
		return
	}

	rf.extendTerm()

	if args.Term > rf.term {
		// find higher term
		rf.transformTo(Follower, args.Term)
	}

	if args.isHeartBeat() {
		// heartbeat message
		(*reply) = AppendEntryReply{appendStateSucc, rf.term, ConflictInfo{}}
		// fmt.Printf("%v -> %v heartbeat, isRecvHB=%v\n", args.LeaderId, rf.me, rf.isRecvHB)
		rf.log.advanceCommited(&args.Commited)
		return
	}

	// append entry at specific location
	state, conflict := rf.appendAt(args.PreEntry, args.Entries)

	// update commited info
	rf.log.advanceCommited(&args.Commited)

	(*reply) = AppendEntryReply{state, rf.term, conflict}
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

	// lock raft and raft.log before call persist!

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)

	e.Encode(rf.log.startAt)
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
	var startAt int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil {

		log.Fatalf("readPersist: error decode")
	} else {
		rf.term = term
		rf.voteFor = voteFor
	}

	var entriesLen int
	// var entriesLen int
	if d.Decode(&startAt) != nil ||
		d.Decode(&entriesLen) != nil {
		log.Fatalf("decode entriesLen failed")
	} else {
		rf.log.startAt = startAt
	}

	entries := make([]logEntry, 0, entriesLen)
	if d.Decode(&entries) != nil {
		log.Fatalf("readPersist: decode entry failed")
	}
	rf.log.entries = entries
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
	Term        int             // term of candidate
	Commited    entryDescriptor // commited info of candidate
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
	rf.log.lock()
	defer rf.log.unlock()

	if rf.voteFor != -1 {
		log.Fatalf("voteForLeader: repeated vote, old=%v, new=%v", rf.voteFor, id)
	}
	rf.voteFor = id

	rf.persist()
}

func (rf *Raft) resetVote() {
	rf.voteFor = -1
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock()
	defer rf.unlock()
	// Your code here (3A, 3B).

	old := rf.term

	// only vote for candidate who has higher term
	if args.Term < rf.term {
		*reply = RequestVoteReply{false, old}
		return
	}

	// commit log entries if possible
	rf.log.advanceCommited(&args.Commited)

	if args.Term > rf.term {
		// find higher term
		rf.transformTo(Follower, args.Term)
	}

	// election restrict. To avoid commited log entry to be overwriten
	//   We need to ensure that new leader should have all commited log entry,
	//   by having current server only vote for a server who has newer entry.
	//   Tips, a server who dont has newest entry could win the election either!
	latest := rf.log.getLatestED()
	if latest.Term > args.Latest.Term || (latest.Term == args.Latest.Term && latest.Index > args.Latest.Index) {
		// fmt.Printf("%v -> %v, reject because %v's log entry[%v] is newer than %v's log entry[%v] \n", rf.me, args.CandidateId, rf.me, latest, args.CandidateId, args.Latest)
		*reply = RequestVoteReply{false, old}
		return
	}

	if rf.voteFor != -1 {
		// have voted this term
		// fmt.Printf("%v -> %v, reject because %v had voted for %v\n", rf.me, args.CandidateId, rf.me, rf.voteFor)
		*reply = RequestVoteReply{false, old}
		return
	}

	rf.extendTerm()

	// vote
	// vote during this term, dont start a new election recently
	rf.voteForLeader(args.CandidateId)

	*reply = RequestVoteReply{true, old}
}

func (rf *Raft) matchIndexStr() string {
	var str string
	for i := 0; i < len(rf.peers); i++ {
		str = str + fmt.Sprintf("%v ", atomic.LoadInt32(&rf.matchIndex[i]))
	}
	return str[:len(str)-1]
}

// send entry(eIndex) to peer(id).
// return next index of log entry need to send
func (rf *Raft) copyEntryTo(id int, thisTerm int, preEntry entryDescriptor, entries []logEntry, doCheck chan bool) {
	if preEntry.Index < 0 {
		log.Fatalf("copyEntryTo: invalid log entry")
	}

	if preEntry.Term == -1 {
		log.Fatalf("copyEntryTo:get previous log entry failed")
	}

	args := AppendEntryArgs{rf.me, entries, preEntry, thisTerm, rf.log.getMaxCommitedED()}
	reply := AppendEntryReply{}

	// fmt.Printf("%v -> %v, copy entry, args=[%v]\n", rf.me, id, args)

	// send log entry until receive reply
	for !rf.killed() {
		if thisTerm != rf.getTerm() {
			// fmt.Printf("%v -> %v, copy entry faild(loop)\n", rf.me, id)
			return
		}

		if rf.peers[id].Call("Raft.AppendEntries", &args, &reply) {
			break
		}
		// overtime, retry until term changed
	}

	rf.lock()
	defer rf.unlock()

	if reply.Term > rf.term {
		// find higher term
		// fmt.Printf("%v -> %v, copy entry faild, higher term, this:%v, cur:%v, peer:%v\n", rf.me, id, thisTerm, rf.term, reply.Term)
		rf.transformTo(Follower, reply.Term)
		rf.extendTerm()
		return
	}

	if thisTerm != rf.term {
		// term changed, let new worker handle nextIndex and matchIndex
		// fmt.Printf("%v -> %v, copy entry faild, term changed: %v to %v\n", rf.me, id, thisTerm, rf.me)
		return
	}

	// get index of next log entry need to be sended.
	switch reply.State {
	case appendStateSucc:
		rf.nextIndex[id] = preEntry.Index + 1 + len(entries)
		atomic.StoreInt32(&rf.matchIndex[id], int32(preEntry.Index+len(entries)))

		// fmt.Printf("%v -> %v, append succ, next Send %v, update match to %v\n", rf.me, id, rf.nextIndex[id], atomic.LoadInt32(&rf.matchIndex[id]))
		doCheck <- true
	case appendStateRepeated:
		rf.nextIndex[id] = preEntry.Index + 1 + 1
		atomic.StoreInt32(&rf.matchIndex[id], int32(preEntry.Index+1))

		// fmt.Printf("%v -> %v, repeated entry, next Send %v, update match to %v\n", rf.me, id, rf.nextIndex[id], atomic.LoadInt32(&rf.matchIndex[id]))
		doCheck <- true
	case appendStateIgnore:
		log.Fatalf("copyEntryTo: should not be here\n")
	case appendStatePrevNotExist:
		if preEntry.Index < reply.Conflict.LogLen {
			log.Fatalf("copyEntryTo: previous entry exist but return appendStatePrevNotExist")
		}

		// follower log entry with preIndex is not exist, send log entry with index=conflict.LogLen
		rf.nextIndex[id] = reply.Conflict.LogLen
	case appendStatePrevConflict:
		// every server should have log entry with index=0
		// so append index=1 shouldnt cause appendStatePrevNotExist
		if preEntry.Index == 0 {
			log.Fatalf("why append the first log entry failed?")
		}

		// fmt.Printf("%v -> %v, previous entry is not exist ", rf.me, id)

		// optimization, bypass nextSendIndex by add more information in reply
		if reply.Conflict.MinIndex > preEntry.Index || reply.Conflict.MinIndex <= 0 || reply.Conflict.Term <= 0 {
			// boundary check
			log.Fatalf("copyEntryTo: copy may would not stop")
		}

		// suppose that log entries in current server whose index in [b, e) have same term which is conflict.Term
		// choose min(b, conflict.MinIndex) as nextSendIndex if b != e, else choose conflict.MinIndex
		rf.log.rLock()
		b, _ := rf.log.equal(reply.Conflict.Term, preEntry.Index)
		rf.log.rUnLock()

		if b == -1 || reply.Conflict.MinIndex < b {
			rf.nextIndex[id] = reply.Conflict.MinIndex
			// fmt.Printf("send %v next\n", rf.nextIndex[id])
			return
		}

		rf.nextIndex[id] = b
		// fmt.Printf("send %v next\n", rf.nextIndex[id])
	default:
		log.Fatalf("invalid appendState:%v", reply.State)
	}
}

// loop to copy entry to server(id) until new term
func (rf *Raft) copyWorker(id int, thisTerm int, doCheck chan bool) {
	for !rf.killed() {
		if thisTerm != rf.getTerm() {
			// term changed, thread of sending request changed, current thread should exit
			doCheck <- false
			return
		}

		if rf.nextIndex[id] == -1 {
			log.Fatalf("copyWorker: nextSend should not be -1 here")
		}

		pre, entries, err := rf.log.entriesAfter(rf.nextIndex[id] - 1)
		if err != nil || len(entries) == 0 {
			// try get valid entry next 10ms
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.copyEntryTo(id, thisTerm, pre, entries, doCheck)
	}
}

// copy log entry to all other server
func (rf *Raft) copyEntry(thisTerm int) {
	// fmt.Printf("me=%v start to copy, thisTerm = %v\n", rf.me, thisTerm)

	doCheck := make(chan bool, len(rf.peers))
	exitCnts := 0

	// one thread per peer server to send log entry
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.copyWorker(i, thisTerm, doCheck)
	}

	// FIXME: how to reach consensus in 2 sec ?
	// 1. speed up election
	// 2. speed up copy

	// same Raft could run this function in two thread with different thisTerm.

	until := len(rf.peers) - 1

	// genenert result of copying entry and update commited info.
	for !rf.killed() {
		ok := <-doCheck
		if !ok {
			exitCnts++

			if exitCnts == until {
				return
			}
		}

		rf.advanceCommited(thisTerm)
	}
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

	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.append(&logEntry{rf.term, command})

	return index, rf.term, true
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

func (rf *Raft) sendHeartBeat(term int) {
	for i, c := range rf.peers {
		// send heartbeat to other server
		if i == rf.me {
			continue
		}

		go func(client *labrpc.ClientEnd) {
			args := AppendEntryArgs{rf.me, []logEntry{}, entryDescriptor{}, term, rf.log.getMaxCommitedED()}
			reply := AppendEntryReply{}
			if client.Call("Raft.AppendEntries", &args, &reply) {
				rf.lock()
				if rf.term < reply.Term {
					// discover higher term, transform to follower
					rf.transformTo(Follower, reply.Term)
				}
				rf.unlock()
			}
		}(c)
	}
}

// send request vote to each server, and fetch result until receive enough tickets either agree or reject
// first return value is true if receive enough agree, false if receive enough reject
// second return value is the max term of all the response
func (rf *Raft) fetchVotesUntil(term int, agreeMin int, rejectMax int) (int, int) {
	voteCh := make(chan bool, len(rf.peers)-1)
	serverTerms := make(chan int, len(rf.peers)-1)

	// get votes from other server
	// fmt.Printf("%v begin request vote, rf=[%v]\n", rf.me, rf)
	for i := 0; i < len(rf.peers); i++ {
		// send heartbeat to other server
		if i == rf.me {
			continue
		}

		go func(client *labrpc.ClientEnd, index int) {
			args := RequestVoteArgs{rf.me, term, rf.machine.getApplied(), rf.log.getLatestED()}
			reply := RequestVoteReply{}
			// labrpc.go may block here rather than timeout,
			if ok := client.Call("Raft.RequestVote", &args, &reply); ok {
				// fmt.Printf("%v receive vote from %v, reply=[%v]\n", rf.me, index, reply)
				voteCh <- reply.VoteGranted
				serverTerms <- reply.Term
			} else {
				// fmt.Printf("%v do not receive vote from %v, reply=[%v]\n", rf.me, index, reply)
				voteCh <- false
				serverTerms <- 0
			}
		}(rf.peers[i], i)
	}

	// check result
	agress := 1
	reject := 0
	maxTerm := 0
	for i := 0; i < len(rf.peers)-1; i++ {
		voted := <-voteCh
		t := <-serverTerms
		if voted {
			agress++
		} else {
			reject++
		}
		if t > maxTerm {
			maxTerm = t
		}

		if agress >= agreeMin || reject >= rejectMax {
			break
		}
	}

	return agress, maxTerm
}

func (rf *Raft) agreeMin() int {
	// len(rf.peers)  agreeMin()
	//            3           2
	//            5           3
	//            6           4
	return (len(rf.peers) + 1 + 1) / 2
}

func (rf *Raft) rejectMax() int {
	// len(rf.peers)   rejectMax()
	//            5             3
	//            6             3
	return (len(rf.peers) + 1) / 2
}

// start election to be the leader of term
func (rf *Raft) startElection() bool {
	rf.lock()
	if rf.state == Leader {
		rf.unlock()
		return false
	}

	if rf.isRecvHB {
		rf.isRecvHB = false
		rf.unlock()
		return false
	}

	rf.transformTo(Candidate, rf.term+1)
	rf.voteForLeader(rf.me)
	thisTerm := rf.term

	// fmt.Printf("me=%v, term %v election start, isRecvHB=%v\n", rf.me, thisTerm, rf.isRecvHB)

	rf.unlock()

	agrees, maxTerm := rf.fetchVotesUntil(thisTerm, rf.agreeMin(), rf.rejectMax())

	// transform state of server according to vote result
	rf.lock()
	defer rf.unlock()

	// fmt.Printf("me=%v, term %v election done, agrees=%v, maxTerm=%v, ", rf.me, thisTerm, agrees, maxTerm)

	if maxTerm > rf.term {
		// find higher term
		// fmt.Printf("lose, find higher term\n")
		rf.transformTo(Follower, maxTerm)
		rf.extendTerm()
		return false
	}

	// term of server changed, old election invalid
	if thisTerm != rf.term {
		// fmt.Printf("lose, term changed\n")
		return false
	}

	if agrees < rf.agreeMin() {
		// fmt.Printf("lose, dont get %v ticket\n", rf.agreeMin())
		return false
	}

	// fmt.Printf("win\n")
	rf.transformTo(Leader, rf.term)
	return true
}

// lock outside
func (rf *Raft) transformTo(newState RaftStateType, newTerm int) {
	// some transform check
	if newState == Leader {
		if rf.state != Candidate {
			log.Fatalf("transformTo: why a follower can transform to leader")
		}

		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int32, len(rf.peers))

		// dectect if we can send a entry
		nextIndex := rf.log.getLatestED().Index
		if nextIndex == 0 {
			// valid log entry start at 1
			nextIndex = 1
		}
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = nextIndex
			rf.matchIndex[i] = 0
		}

		go rf.heartbeat(newTerm)
		go rf.copyEntry(newTerm)
	} else if newState == Follower {
		if newTerm <= rf.term {
			log.Fatalf("transformTo: transform to leader should have higher term")
		}

		rf.resetVote()
	} else {
		if rf.state == Leader {
			log.Fatalf("transformTo: why leader transform to a candidate")
		}

		if newTerm <= rf.term {
			log.Fatalf("transformTo: transform to candidate should have higher term")
		}

		rf.resetVote()
		rf.extendTerm()
	}

	rf.term = newTerm
	if newState != Leader {
		// transform to candidate, follower will change term, so persist
		rf.log.lock()
		rf.persist()
		rf.log.unlock()
	}

	rf.state = newState
}

func (rf *Raft) heartbeat(thisTerm int) {
	for !rf.killed() {
		curTerm := rf.getTerm()
		if curTerm != thisTerm {
			break
		}

		// send heartbeat message
		rf.sendHeartBeat(curTerm)

		// heartbeat timeout must below than term timeout
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) electionPeriod() {
	for !rf.killed() {
		go rf.startElection()

		ms := 100 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLogEntry() {
	// apply log whose index in (maxApplyIndex, maxCommitedIndex] periodically
	for !rf.killed() {
		maxApply := rf.machine.getApplied().Index
		maxCommited := rf.log.getMaxCommitedED().Index
		if maxApply > maxCommited {
			log.Fatalf("applyLogEntry: maxApply > maxCommited, why apply some uncommited log entry")
		}

		rf.log.rLock()

		for i := maxApply + 1; i <= maxCommited; i++ {
			if !rf.log.isIndexExist(i) {
				log.Fatalf("applyLogEntry: why commited log was removed")
			}
			entry := rf.log.entries[rf.log.slotId(i)]
			rf.machine.applyLogEntry(i, entry)
		}

		rf.log.rUnLock()

		time.Sleep(10 * time.Millisecond)
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

	// start ticker goroutine to start elections
	go rf.electionPeriod()
	go rf.applyLogEntry()

	return rf
}
