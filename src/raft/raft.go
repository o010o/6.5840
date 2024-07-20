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

	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	appendStateRepeated     AppendStateType = 3 // append failed because entry had been in log
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

func (m *stateMachine) applyLogEntry(e *logEntry, index int) {
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

func (e *logEntry) isHeartBeat() bool {
	return e.Command == nil
}

//----------------------------- logEntry --------------------------------

//----------------------------- raftLog --------------------------------

type raftLog struct {
	mu sync.RWMutex
	// apply may block because of net, so we have to try apply log entry until success. So just send log entry periodically.
	// cond             sync.Cond   // signal there are new commited entries
	entries          []*logEntry // pointer to content of log entry
	startAt          int         // the index of first entry in entries
	maxCommitedIndex int         // biggest index which can apply to state machine safely.
	// latestCommited entryDescriptor // latest commited entry
}

func (rl *raftLog) advanceCommited(commited *entryDescriptor) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if !rl.isEntryExist(commited) {
		return false
	}

	if rl.maxCommitedIndex < commited.Index {
		rl.maxCommitedIndex = commited.Index
	}

	return true
}

func (rl *raftLog) construct() {
	rl.entries = make([]*logEntry, 1)
	rl.entries[0] = &logEntry{0, nil}
	rl.startAt = 0
	rl.maxCommitedIndex = 0
}

func (rl *raftLog) getMaxCommitedIndex() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return rl.maxCommitedIndex
}

func (rl *raftLog) getLatestED() entryDescriptor {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

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

func (rl *raftLog) append(e *logEntry) int {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	newIndex := len(rl.entries)

	rl.entries = append(rl.entries, e)

	return newIndex
}

// insert e after entry(index, term)
// return false if entry(index, term) is not exist
func (rl *raftLog) appendAt(pIndex int, pTerm int, e *logEntry) AppendStateType {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if pIndex < 0 {
		log.Fatalf("pIndex should greater or equal than 0")
	}

	if !rl.isEntryExist(&entryDescriptor{pIndex, pTerm}) {
		// previous log entry should exist
		return appendStatePrevNotExist
	}

	if rl.isEntryExist(&entryDescriptor{pIndex + 1, e.Term}) {
		// Entry will remove valid entry if **past log entry** of current term comes.
		// so ignore repeated log entry
		return appendStateRepeated
	}

	if pIndex < rl.maxCommitedIndex {
		// Raft implement failed because server remove some commited log entry
		log.Fatalf("appendAt: why remove commited log entry? maxCommitedIndex=%v, pre=[index=%v, term=%v], append=%v", rl.maxCommitedIndex, pIndex, pTerm, *e)
	}

	rl.entries = append(rl.entries[:rl.slotId(pIndex)+1], e)

	return appendStateSucc
}

// return slot id of index
func (rl *raftLog) slotId(index int) int {
	// used before lock!
	return index - rl.startAt
}

func (rl *raftLog) lastIndex() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return rl.startAt + len(rl.entries) - 1
}

func (rl *raftLog) copyEntry(index int) (logEntry, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if !rl.isIndexExist(index) {
		return logEntry{-1, nil}, errors.New("invalid index")
	}

	return *rl.entries[rl.slotId(index)], nil
}

//-------------------------------- raftLog ----------------------------------

//-------------------------------- Raft -------------------------------------

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	term      int                 // term of current server
	voteAt    int                 // term of latest vote
	state     RaftStateType       // state of current server
	isRecvHB  bool                // did this server receive heartbeat recently?
	log       raftLog             // content of log entry
	machine   stateMachine        // state machine
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) String() string {
	return fmt.Sprintf("me:%v, term:%v, state:%v,  isRecvHB:%v", rf.me, rf.term, rf.state, rf.isRecvHB)
}

func (rf *Raft) construct(applyCh chan ApplyMsg) {
	rf.term = 0
	rf.state = Follower
	rf.isRecvHB = true

	rf.log.construct()
	rf.machine.construct(applyCh)

	// start ticker goroutine to start elections
	go rf.heartbeat()
	go rf.electionPeriod()
	go rf.applyLogEntry()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	return rf.term, rf.state == Leader
}

func (rf *Raft) GetRaftState() RaftStateType {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state
}

//-------------------------------- Raft -------------------------------------

func (rf *Raft) extendTerm() {
	// cause ticker not to call election this recycle
	rf.isRecvHB = true
}

func (rf *Raft) getTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.term
}

type entryDescriptor struct {
	Index int
	Term  int
}

type AppendEntryArgs struct {
	E        logEntry        // entry need to be appended
	Index    int             // index of entry E
	PreTerm  int             // term of previous entry of E
	Term     int             // term of server
	Commited entryDescriptor // latest entry Commited to state machine
}

func (aep *AppendEntryArgs) String() string {
	return fmt.Sprintf("curEntry=[%v, %v], preEntry=[%v, %v], serverTerm=%v, commited=[%v, %v]", aep.Index, aep.E.Term, aep.Index-1, aep.PreTerm, aep.Term, aep.Commited.Index, aep.Commited.Term)
}

type AppendEntryReply struct {
	State AppendStateType // 0:succ, 1:lower term, 2:
	Term  int             // term of peer server
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.term > args.Term {
		// caller find higher term
		(*reply) = AppendEntryReply{appendStateIgnore, rf.term}
		return
	}

	rf.extendTerm()
	// update commited info
	rf.log.advanceCommited(&args.Commited)

	if args.Term > rf.term {
		// find higher term
		rf.transformTo(Follower, args.Term)
	}

	if args.E.isHeartBeat() {
		// heartbeat message
		(*reply) = AppendEntryReply{appendStateSucc, rf.term}
		return
	}

	// append entry at specific location
	state := rf.log.appendAt(args.Index-1, args.PreTerm, &args.E)
	(*reply) = AppendEntryReply{state, rf.term}
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
	Term     int             // term of candidate
	Commited entryDescriptor // commited info of candidate
	Latest   entryDescriptor
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Ok   bool
	Term int // term of server which sends reply
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).

	// only vote for candidate who has higher term
	if args.Term < rf.term || args.Term == rf.voteAt {
		*reply = RequestVoteReply{false, rf.term}
		return
	}

	// commit log entries if possible
	rf.log.advanceCommited(&args.Commited)

	old := rf.term

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
		*reply = RequestVoteReply{false, old}
		return
	}

	// vote
	// vote during this term, dont start a new election recently
	rf.extendTerm()

	rf.voteAt = rf.term
	*reply = RequestVoteReply{true, old}
}

// send entry(eIndex) to peer(id).
// return next index of log entry need to send
func (rf *Raft) copyEntryTo(id int, thisTerm int, eIndex int, entry *logEntry, succEntryCh chan entryDescriptor) int {
	if eIndex == 0 {
		log.Fatalf("log entry of 0 is invalid")
	}

	pre, _ := rf.log.copyEntry(eIndex - 1)
	if pre.Term == -1 {
		log.Fatalf("copyEntryTo:get previous log entry failed")
	}

	args := AppendEntryArgs{*entry, eIndex, pre.Term, thisTerm, rf.machine.getApplied()}
	reply := AppendEntryReply{}

	// send small commited log entry first to avoid follower can not catch up leader
	maxCommited := rf.log.getMaxCommitedIndex()
	if eIndex < maxCommited {
		// always send latest applied log entry may
		//  cause follower can not catch up leader
		args.Commited.Index = eIndex
		args.Commited.Term = entry.Term
	}

	// send log entry until receive reply
	for {
		if thisTerm != rf.getTerm() {
			return -1
		}

		if rf.peers[id].Call("Raft.AppendEntries", &args, &reply) {
			fmt.Printf("me=%v, call Raft.AppendEntries succ, args=[%v], reply=[%v]\n", rf.me, args, reply)
			break
		}
		// overtime, retry until term changed
	}

	// TODO:optimization, bypass nextSendIndex by add more information in reply
	// get index of next log entry need to be sended.
	switch reply.State {
	case appendStateSucc:
		succEntryCh <- entryDescriptor{eIndex, entry.Term}
		return eIndex + 1
	case appendStateRepeated:
		return eIndex + 1
	case appendStateIgnore:

		rf.mu.Lock()
		if rf.term < reply.Term {
			// find higher term
			rf.transformTo(Follower, reply.Term)
		}
		rf.mu.Unlock()

		return -1
	case appendStatePrevNotExist:
		// every server should have log entry with index=0
		// so append index=1 shouldnt cause appendStatePrevNotExist
		if eIndex == 1 {
			log.Fatalf("why append the first log entry failed?")
		}
		return eIndex - 1
	default:
		log.Fatalf("invalid appendState:%v", reply.State)
		return -1
	}
}

// loop to copy entry to server(id) until new term
func (rf *Raft) copyWorker(id int, thisTerm int, nextSend int, succEntryCh chan entryDescriptor) {
	fmt.Printf("me=%v, create worker, id=%v, thisTerm:%v, nextSend=%v\n", rf.me, id, thisTerm, nextSend)

	for {
		if thisTerm != rf.getTerm() {
			// term changed, thread of sending request changed, current thread should exit
			succEntryCh <- entryDescriptor{-1, -1}
			return
		}

		if nextSend == -1 {
			log.Fatalf("copyWorker: nextSend should not be -1 here")
		}

		entry, err := rf.log.copyEntry(nextSend)
		if err != nil {
			// wait for log entry
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// test
		fmt.Printf("me=%v, copy entry to peer(%v), entry=%v\n", rf.me, id, entry)

		nextSend = rf.copyEntryTo(id, thisTerm, nextSend, &entry, succEntryCh)
	}
}

// copy log entry to all other server
func (rf *Raft) copyEntry(thisTerm int) {
	peersTotal := len(rf.peers) - 1

	// nextSend init with lastIndex rather than lastIndex + 1 because we
	// want to start send log entry without having to append a new log entry
	nextSend := rf.log.lastIndex()
	if nextSend == 0 {
		// no need to send log entry with index = 0
		nextSend = 1
	}

	succEntryCh := make(chan entryDescriptor, peersTotal)
	cnts := make(map[int]int)
	exitCnt := 0

	fmt.Printf("me=%v, copyEntry, peersTotal=%v nextSend=%v\n", rf.me, peersTotal, nextSend)

	// one thread per peer server to send log entry
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.copyWorker(i, thisTerm, nextSend, succEntryCh)
	}

	// genenert result of copying entry and update commited info.
	for {
		if exitCnt == peersTotal {
			// exit when all worker exit, a woker will exit if it detect term changed
			fmt.Printf("me=%v, all worker done\n", rf.me)
			return
		}

		// generate successful copy then update max commited index
		ed := <-succEntryCh

		if ed.Index < 0 || ed.Term < 0 {
			// exist when all worker exit
			exitCnt = exitCnt + 1
			continue
		}

		if ed.Index == 0 || ed.Term == 0 {
			log.Fatalf("log entry(0) should not be sended")
		}

		cnts[ed.Index] = cnts[ed.Index] + 1

		fmt.Printf("me=%v, %v send successfully, cnts=%v\n", rf.me, ed.Index, cnts)

		if cnts[ed.Index] == rf.agreeMin()-1 {
			if ed.Term == thisTerm {
				// Only commit log entry created by this leader, previous log entry was commited implictly.
				// Previous log entry may be overwriten if we count times.

				rf.log.advanceCommited(&ed)
			}
		} else if cnts[ed.Index] == peersTotal {
			delete(cnts, ed.Index)
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	index := rf.log.append(&logEntry{rf.term, command})

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

func (rf *Raft) sendHeartBeat() {
	term := rf.getTerm()

	for i, c := range rf.peers {
		// send heartbeat to other server
		if i == rf.me {
			continue
		}

		go func(client *labrpc.ClientEnd) {
			args := AppendEntryArgs{logEntry{term, nil}, 0, 0, term, rf.machine.getApplied()}
			reply := AppendEntryReply{}
			if client.Call("Raft.AppendEntries", &args, &reply) {
				rf.mu.Lock()
				if rf.term < reply.Term {
					// discover higher term, transform to follower
					rf.transformTo(Follower, reply.Term)
				}
				rf.mu.Unlock()
			}
		}(c)
	}
}

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
			args := RequestVoteArgs{term, rf.machine.getApplied(), rf.log.getLatestED()}
			reply := RequestVoteReply{}
			// labrpc.go may block here rather than timeout,
			if ok := client.Call("Raft.RequestVote", &args, &reply); ok {
				fmt.Printf("%v receive vote from %v, reply=[%v]\n", rf.me, index, reply)
				voteCh <- reply.Ok
				serverTerms <- reply.Term
			} else {
				fmt.Printf("%v do not receive vote from %v, reply=[%v]\n", rf.me, index, reply)
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
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return false
	}
	rf.transformTo(Candidate, rf.term+1)
	term := rf.term
	rf.voteAt = term

	fmt.Printf("me=%v, start to election, term=%v, rf=[%v]\n", rf.me, rf, term)
	rf.mu.Unlock()

	agrees, maxTerm := rf.fetchVotesUntil(term, rf.agreeMin(), rf.rejectMax())

	// transform state of server according to vote result
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("me=%v, term %v election done, agrees=%v, maxTerm=%v, rf=[%v] ", rf.me, term, agrees, maxTerm, rf)

	if maxTerm > rf.term {
		// find higher term
		fmt.Printf("lose, find higher term\n")
		rf.transformTo(Follower, maxTerm)
		return false
	}

	// term of server changed, old election invalid
	if term != rf.term {
		fmt.Printf("lose, term changed\n")
		return false
	}

	if agrees < rf.agreeMin() {
		fmt.Printf("lose, dont get %v ticket\n", rf.agreeMin())
		return false
	}

	fmt.Printf("win\n")
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
		go rf.copyEntry(newTerm)
	} else if newState == Follower {
		if newTerm <= rf.term {
			log.Fatalf("transformTo: transform to leader should have higher term")
		}

		rf.extendTerm()
	}

	rf.term = newTerm
	rf.state = newState
}

func (rf *Raft) isTermExpired() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	timeout := ((!rf.isRecvHB) && (rf.state != Leader))
	rf.isRecvHB = false

	return timeout
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		// send heartbeat message if current server is a leader!
		if rf.GetRaftState() == Leader {
			rf.sendHeartBeat()
		}

		// heartbeat timeout must below than term timeout
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) electionPeriod() {
	for !rf.killed() {
		// Your code here (3A)

		if rf.isTermExpired() {
			go rf.startElection()
		}

		// milliseconds.120~250
		ms := 100 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLogEntry() {
	// apply log whose index in (maxApplyIndex, maxCommitedIndex] periodically
	for !rf.killed() {
		maxApply := rf.machine.getApplied().Index
		maxCommited := rf.log.getMaxCommitedIndex()
		if maxApply > maxCommited {
			log.Fatalf("applyLogEntry: maxApply > maxCommited, why apply some uncommited log entry")
		}

		for i := maxApply + 1; i <= maxCommited; i++ {
			entry, err := rf.log.copyEntry(i)
			if err != nil {
				log.Fatalf("why log was removed?")
			}
			rf.machine.applyLogEntry(&entry, i)
		}

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

	return rf
}
