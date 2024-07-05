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

type RaftStateType uint8

const (
	Follower  RaftStateType = 1
	Candidate RaftStateType = 2
	Leader    RaftStateType = 3
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	term     int           // term of current server
	state    RaftStateType // state of current server
	isRecvHB bool          // did this server receive heartbeat recently?

	// election
	// heartbeat

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) String() string {
	return fmt.Sprintf("me:%v, term:%v, state:%v,  isRecvHB:%v", rf.me, rf.term, rf.state, rf.isRecvHB)
}

func (rf *Raft) Construct() {
	rf.term = 0
	rf.state = Follower
	rf.isRecvHB = false
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

type LogEntry struct {
	Term  int
	Index int
	// heartbeat message if value is nil
	Value interface{}
}

func (e *LogEntry) isHeartBeat() bool {
	return e.Value == nil
}

type AppendEntryArgs struct {
	E LogEntry
	// Log Matching require
	PTerm  int
	PIndex int
}

type AppendEntryReply struct {
	// term of peer server
	Ok   bool
	Term int
}

func (rf *Raft) updateTerm(newTerm int) bool {
	if rf.term > newTerm {
		log.Fatalf("can not decrease term")
	}
	rf.term = newTerm
	return true
}

func (rf *Raft) extendTerm() {
	// cause ticker not to call election this recycle
	rf.isRecvHB = true
}

func (rf *Raft) getTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.term
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.term > args.E.Term {
		// caller find higher term
		(*reply) = AppendEntryReply{false, rf.term}
		return
	}

	rf.extendTerm()

	if args.E.isHeartBeat() {
		// heartbeat message
		rf.transformTo(Follower)
		(*reply) = AppendEntryReply{true, rf.term}
		rf.updateTerm(args.E.Term)
		return
	}

	// TODO:append log entry
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Ok   bool
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	if args.Term <= rf.term {
		*reply = RequestVoteReply{false, rf.term}
		return
	}

	// vote
	old := rf.term
	rf.updateTerm(args.Term)
	rf.transformTo(Follower)
	rf.extendTerm()

	*reply = RequestVoteReply{true, old}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
			args := AppendEntryArgs{LogEntry{term, 0, nil}, 0, 0}
			reply := AppendEntryReply{}
			if client.Call("Raft.AppendEntries", &args, &reply) {
				rf.mu.Lock()
				if rf.term < reply.Term {
					// discover higher term, transform to follower
					rf.transformTo(Follower)
					rf.updateTerm(reply.Term)
				}
				rf.mu.Unlock()
			}
		}(c)
	}
}

func (rf *Raft) fetchVotesUntil(term int, agreeMin int, rejectMax int) (int, int) {
	voteRes := make(chan bool, len(rf.peers)-1)
	serverTerms := make(chan int, len(rf.peers)-1)

	// get votes from other server
	// fmt.Printf("%v begin request vote, rf=[%v]\n", rf.me, rf)
	for i := 0; i < len(rf.peers); i++ {
		// send heartbeat to other server
		if i == rf.me {
			continue
		}

		go func(client *labrpc.ClientEnd, index int) {
			args := RequestVoteArgs{term}
			reply := RequestVoteReply{}
			// labrpc.go may block here rather than timeout,
			if ok := client.Call("Raft.RequestVote", &args, &reply); ok {
				// fmt.Printf("%v receive vote from %v, reply=[%v]\n", rf.me, index, reply)
				voteRes <- reply.Ok
				serverTerms <- reply.Term
			} else {
				// fmt.Printf("%v do not receive vote from %v, reply=[%v]\n", rf.me, index, reply)
				voteRes <- false
				serverTerms <- 0
			}
			// if we discover higher term or there is a leader, for simplify, dont change state now, change it until heartbeat from leader.
		}(rf.peers[i], i)
	}

	// check result
	agress := 1
	reject := 0
	maxTerm := 0
	for i := 0; i < len(rf.peers)-1; i++ {
		res := <-voteRes
		t := <-serverTerms
		if res {
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

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return false
	}
	rf.transformTo(Candidate)
	rf.updateTerm(rf.term + 1)
	term := rf.term
	rf.mu.Unlock()

	// FIXME: election still too slow, can we use asynchronous way
	// win if get over half votes
	agreeMin := (len(rf.peers) + 1 + 1) / 2
	rejectMax := (len(rf.peers) + 1) / 2
	agrees, maxTerm := rf.fetchVotesUntil(term, agreeMin, rejectMax)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Follower {
		return false
	}

	if maxTerm > rf.term {
		// fmt.Printf("%v discover a higher term, rf=[%v]\n", rf.me, rf)
		rf.transformTo(Follower)
		rf.updateTerm(maxTerm)
	} else if agrees >= agreeMin {
		// fmt.Printf("%v win, rf=[%v]\n", rf.me, rf)
		rf.transformTo(Leader)
	} else {
		// fmt.Printf("%v lose, rf=[%v]\n", rf.me, rf)
	}

	return true
}

// lock outside
func (rf *Raft) transformTo(t RaftStateType) {
	rf.state = t
}

func (rf *Raft) checkTermTimeout() bool {
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

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)

		if rf.checkTermTimeout() {
			go rf.startElection()
		}

		// milliseconds.120~250
		ms := 100 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

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
	rf.Construct()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.heartbeat()
	go rf.ticker()

	return rf
}
