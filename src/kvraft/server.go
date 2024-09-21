package kvraft

import (
	"bytes"
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
	Debug                           = false
	OpPut             OpType        = 0
	OpAppend          OpType        = 1
	OpGet             OpType        = 2
	CheckTermInterval time.Duration = 100 * time.Millisecond
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

type notify struct {
	op    *Op
	index int
	term  int
	ch    chan *execResult
}

func (n *notify) isEmpty() bool {
	return n.index <= 0
}

func (n *notify) init(op *Op, index int, term int, ch chan *execResult) {
	n.op = op
	n.index = index
	n.term = term
	n.ch = ch
}

func (n *notify) reset() {
	n.op = nil
	n.index = -1
	n.term = -1
	n.ch = nil
}

type opMessage struct {
	op       *Op
	resultCh chan *execResult
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	database     KVDatabase
	newOpCh      chan *opMessage
	opHistory    operationHistory
}

func (kv *KVServer) execDispatch(op *Op) (interface{}, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	resultCh := make(chan *execResult, 1)
	kv.newOpCh <- &opMessage{op, resultCh}

	res := <-resultCh

	return res.result, res.err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// handle operation one by one
	DPrintf("me=%v, Get, args=%v", kv.me, args)

	op := Op{args.Id, OpGet, args.Key, ""}
	value, err := kv.execDispatch(&op)
	if err == OK {
		v, ok := value.(string)
		if !ok {
			panic("execDispatch return true with empty result")
		}
		*reply = GetReply{OK, v}
	} else {
		*reply = GetReply{err, ""}
	}

	DPrintf("me=%v, result={%v}", kv.me, value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// handle operation one by one
	DPrintf("me=%v, Put, args=%v", kv.me, args)

	op := Op{args.Id, OpPut, args.Key, args.Value}
	r, err := kv.execDispatch(&op)
	*reply = PutAppendReply{err}

	DPrintf("me=%v, result={%v}", kv.me, r)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// handle operation one by one
	DPrintf("me=%v, Append, args=%v", kv.me, args)

	op := Op{args.Id, OpAppend, args.Key, args.Value}
	r, err := kv.execDispatch(&op)
	*reply = PutAppendReply{err}

	DPrintf("me=%v, result={%v}", kv.me, r)
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

type execResult struct {
	err    Err
	result interface{}
}

func (kv *execResult) String() string {
	return fmt.Sprintf("Err=\"%v\", result=\"%v\"", kv.err, kv.result)
}

func (kv *KVServer) handleCommand(msg *raft.ApplyMsg) execResult {
	// execute command if the command is not repeated, or fetch result from history
	op, ok := msg.Command.(Op)
	if !ok {
		panic("not a valid command")
	}

	// Sends response if write(Append(), Put()) has been executed. Re-executes read(Get()) request.
	if op.T != OpGet && kv.opHistory.find(&op) {
		return execResult{OK, nil}
	}

	r := kv.execOp(msg.CommandIndex, &op)

	kv.opHistory.insert(&op, &r)

	return r
}

func (kv *KVServer) execOp(index int, op *Op) execResult {
	switch op.T {
	case OpGet:
		value, err := kv.database.get(index, op.Key)
		if err != nil {
			return execResult{ErrNoKey, nil}
		}
		return execResult{OK, value}
	case OpPut:
		kv.database.put(index, op.Key, op.Value)
	case OpAppend:
		kv.database.append(index, op.Key, op.Value)
	default:
		panic("execOp: unknown operation")
	}
	return execResult{OK, nil}
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

func (kv *KVServer) tryNotify(msg *raft.ApplyMsg, result *execResult, no *notify) {
	if no == nil || result == nil || no.isEmpty() {
		return
	}
	// notify waiting thread if
	// 1. term changed
	// 2. executing operation done
	term, isLeader := kv.rf.GetState()
	if term != no.term {
		no.ch <- &execResult{ErrWrongLeader, nil}
		no.reset()
		return
	}

	if msg == nil {
		return
	}

	if no.index == msg.CommandIndex {
		if !isLeader {
			panic(fmt.Sprintf("executeOps: why server is not leader at term %v", term))
		}

		no.ch <- result
		no.reset()
		return
	} else if no.index < msg.CommandIndex {
		panic("lost notify")
	}
}

func (kv *KVServer) worker() {
	no := notify{}

	for !kv.killed() {
		select {
		case n := <-kv.newOpCh:
			index, term, isLeader := kv.rf.Start(*n.op)
			if !isLeader {
				n.resultCh <- &execResult{ErrWrongLeader, nil}
			} else {
				no.init(n.op, index, term, n.resultCh)
			}
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

				res := kv.handleCommand(&msg)
				kv.tryNotify(&msg, &res, &no)
			} else if msg.SnapshotValid {
				DPrintf("me=%v, reset database, index=%v, term=%v", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
				kv.applySnapshot(&msg)
			}
		case <-time.After(CheckTermInterval):
			kv.tryNotify(nil, &execResult{ErrWrongLeader, nil}, &no)
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
	kv.newOpCh = make(chan *opMessage, 1)
	//--------------------

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	// create one goroutine to receive message from channel
	go kv.worker()

	return kv
}
