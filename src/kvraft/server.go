package kvraft

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type OpType int

const (
	Debug           = false
	OpPut    OpType = 0
	OpAppend OpType = 1
	OpGet    OpType = 2
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type clientRequestIdentity struct {
	ClientId  int64
	RequestId int64
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id    clientRequestIdentity
	T     OpType
	Key   string
	Value string
}

func (o *Op) String() string {
	typeStrs := map[OpType]string{
		OpPut:    "Put",
		OpGet:    "Get",
		OpAppend: "Append",
	}
	return fmt.Sprintf("id={cId=%v, rId=%v}, t=%v, k=\"%v\", v=\"%v\"", o.Id.ClientId, o.Id.RequestId, typeStrs[o.T], o.Key, o.Value)
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("k=\"%v\", v=\"%v\"", args.Key, args.Value)
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("k=\"%v\"", args.Key)
}

type notice struct {
	op    *Op
	index int
	term  int
	ch    chan execOpResult
}

func (n *notice) empty() bool {
	return n.index <= 0
}

func (n *notice) init(op *Op, index int, term int, ch chan execOpResult) {
	n.op = op
	n.index = index
	n.term = term
	n.ch = ch
}

func (n *notice) reset() {
	n.op = nil
	n.index = -1
	n.term = -1
	n.ch = nil
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	database     KVDatabase
	noticeCh     chan notice
	opHistory    operationHistory
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// handle operation one by one
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("me=%v, Get, args=%v", kv.me, args)

	op := Op{args.Id, OpGet, args.Key, ""}
	ch := make(chan execOpResult, 1)
	done := make(chan bool, 1)

	kv.registerOp(&op, ch, done)
	defer kv.cancelRegisterOp(done)

	// wait until
	// - operation reach consenus and execute operation done.
	// - leader changed
	r := <-ch
	reply.Err = r.Err
	if r.Err == OK {
		reply.Value = r.result.(string)
	}

	DPrintf("me=%v, result={%v}", kv.me, &r)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// handle operation one by one
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("me=%v, Put, args=%v", kv.me, args)

	op := Op{args.Id, OpPut, args.Key, args.Value}
	ch := make(chan execOpResult, 1)
	done := make(chan bool, 1)

	kv.registerOp(&op, ch, done)
	defer kv.cancelRegisterOp(done)

	// wait until
	// - operation reach consenus and execute operation done.
	// - leader changed
	r := <-ch
	reply.Err = r.Err
	DPrintf("me=%v, result={%v}", kv.me, &r)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// handle operation one by one
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("me=%v, Append, args=%v", kv.me, args)

	op := Op{args.Id, OpAppend, args.Key, args.Value}
	ch := make(chan execOpResult, 1)
	done := make(chan bool, 1)

	kv.registerOp(&op, ch, done)
	defer kv.cancelRegisterOp(done)

	// wait until
	// - operation reach consenus and execute operation done.
	// - leader changed
	r := <-ch
	reply.Err = r.Err
	DPrintf("me=%v, result={%v}", kv.me, &r)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

type execOpResult struct {
	Err    Err
	result interface{}
}

func (kv *execOpResult) String() string {
	return fmt.Sprintf("Err=\"%v\", result=\"%v\"", kv.Err, kv.result)
}

func (kv *KVServer) registerTermChanged(oldTerm int, ch chan execOpResult, done chan bool) {
	go func() {
		for !kv.killed() {
			// loop until done is true or term changed

			term, _ := kv.rf.GetState()
			if oldTerm != term {
				DPrintf("me=%v, term changed", kv.me)
				ch <- execOpResult{ErrWrongLeader, nil}
				return
			}

			select {
			case <-done:
				return
			case <-time.After(time.Millisecond * 100):
			}
		}
	}()
}

func (kv *KVServer) registerOp(op *Op, ch chan execOpResult, done chan bool) (int, int, error) {
	index, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		ch <- execOpResult{ErrWrongLeader, nil}
		return -1, -1, errors.New("registerOperation: not leader")
	}

	DPrintf("me=%v, propagate op(%v)", kv.me, op)

	kv.registerTermChanged(term, ch, done)
	kv.noticeCh <- notice{op, index, term, ch}

	return index, term, nil
}

func (kv *KVServer) cancelRegisterOp(done chan bool) {
	done <- true
}

func (kv *KVServer) tryNoticeResponse(msg *raft.ApplyMsg, execResult *execOpResult, notice *notice) {
	if notice == nil || notice.index <= 0 || notice.term <= 0 {
		return
	}

	// Notice the waiting goroutine to response if it is waiting for the current command.
	term, isLeader := kv.rf.GetState()
	if notice.index == msg.CommandIndex && notice.term == term {
		if !isLeader {
			panic(fmt.Sprintf("executeOps: why server is not leader at term %v", term))
		}

		notice.ch <- *execResult
	}

	if msg.CommandIndex >= notice.index {
		notice.reset()
	}
}

func (kv *KVServer) handleCommand(msg *raft.ApplyMsg, notice *notice) {
	// execute command if the command is not repeated, or fetch result from history
	op, ok := msg.Command.(Op)
	if !ok {
		panic("not a valid command")
	}

	// Sends response if write(Append(), Put()) has been executed. Re-executes read(Get()) request.
	if op.T != OpGet && kv.opHistory.find(&op) {
		kv.tryNoticeResponse(msg, &execOpResult{OK, nil}, notice)
		return
	}

	r := kv.execOp(msg.CommandIndex, &op)

	kv.opHistory.insert(&op, &r)

	if !notice.empty() {
		kv.tryNoticeResponse(msg, &r, notice)
	}
}

func (kv *KVServer) execOp(index int, op *Op) execOpResult {
	switch op.T {
	case OpGet:
		value, err := kv.database.get(index, op.Key)
		if err != nil {
			return execOpResult{ErrNoKey, nil}
		}
		return execOpResult{OK, value}
	case OpPut:
		kv.database.put(index, op.Key, op.Value)
	case OpAppend:
		kv.database.append(index, op.Key, op.Value)
	default:
		panic("execOp: unknown operation")
	}
	return execOpResult{OK, nil}
}

func (kv *KVServer) generateSnapshot() (int, []byte) {
	// Snapshot should contain state machine and historical request.
	// State machine should be saved because it will replace the removed log.
	// And we save historical requests because we want to verify whether the resumed state machine has the request identical to a new request.

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	last := kv.database.serialization(e)
	kv.opHistory.serialization(e)

	return last, w.Bytes()
}

func (kv *KVServer) applySnapshot(msg *raft.ApplyMsg) {
	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)

	kv.database.unSerialization(msg.SnapshotIndex, d)
	kv.opHistory.unSerialization(d)
}

func (kv *KVServer) fetchAndExecOp() {
	notice := notice{}

	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid && msg.SnapshotValid {
				panic("executeOps: invalid apply message")
			}

			if msg.CommandValid {
				if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
					index, data := kv.generateSnapshot()
					// index, data := kv.database.serialization()
					DPrintf("me=%v, execute snapshot, index=%v, size=%v, max=%v", kv.me, index, kv.rf.RaftStateSize(), kv.maxraftstate)

					kv.rf.Snapshot(index, data)
				}

				kv.handleCommand(&msg, &notice)
			} else if msg.SnapshotValid {
				DPrintf("me=%v, reset database, index=%v, term=%v", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
				kv.applySnapshot(&msg)
			}

		case n := <-kv.noticeCh:
			// only allow one notice
			notice.init(n.op, n.index, n.term, n.ch)
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	//----my initialization code---
	kv.database.construct()
	kv.opHistory.construct()
	kv.noticeCh = make(chan notice, 1)
	//--------------------

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	// create one goroutine to receive message from channel
	go kv.fetchAndExecOp()

	return kv
}
