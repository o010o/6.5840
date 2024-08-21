package kvraft

import (
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

	// TODO: detect repeated operation here?
	// Not here. We dont know if the incoming message has ever been sent to another server.

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

	// TODO: do not execute repeated write(Put(), Append()) request
	// some scenarioes:
	// 1. Client send same write request many times because it do not receive reply. Resend is done by underly transport protocol.
	// 2. Client send same write request to another server after request had been commited. The request may execute twice at either server.

	// solution:
	// Using solution that is samilar to kvsrv, that is, record last write request, and do nothing if new request is same as the last.
	// Still have some problem if different client have same id. There is another write request commited and reply successfully, so the repeated request would execute twice.
	// if we record all historical write request?

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

	// TODO: speed up!
	// we need ensure that there are 3 ops done during one heartbeat(100ms). But our speed is 100ms per op.
	// There may be some problem:
	// 1. Raft slow.
	// We can reach consenus at averge 100 ms, and the transport delay is 0~27 ms if network if reliable.
	// I think we could not satify the request even if we could reach a consenus within 27 ms.
	// I think the best way to optimise is by executing more operations during one consensus.
	// No, dude. The transport delay is at most 27 ms, so there is something wrong happened to Raft that it needs 100 ms to reach consenus.
	// 2. Server slow.

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

func (kv *KVServer) snapshot() {
	// TODO: do snapshot
}

func (kv *KVServer) snapshotMonitator() {
	// TODO: wait/loop for snapshot
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

	// TODO: Shall we notice if there is a different operation at the index sames as notice.index?
	// NO, i think monitor the term is enough

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

	r := kv.execOp(&op)

	kv.opHistory.insert(&op, &r)

	if !notice.empty() {
		kv.tryNoticeResponse(msg, &r, notice)
	}
}

func (kv *KVServer) execOp(op *Op) execOpResult {
	switch op.T {
	case OpGet:
		value, err := kv.database.get(op.Key)
		if err != nil {
			return execOpResult{ErrNoKey, nil}
		}
		return execOpResult{OK, value}
	case OpPut:
		kv.database.put(op.Key, op.Value)
	case OpAppend:
		kv.database.append(op.Key, op.Value)
	default:
		panic("execOp: unknown operation")
	}
	return execOpResult{OK, nil}
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
				// TODO: detect repeated operation here
				// Dont do anything if we receive a repeated operation

				kv.handleCommand(&msg, &notice)
			} else if msg.SnapshotValid {
				// TODO:
				// - replace content of data using snapshot
			}

			// TODO: support creating new snapshot if log is too big
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
	// TODO: create a goroutine that snapshot replica state machine when raft state reach maxraftstate

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
