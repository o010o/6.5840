package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type history struct {
	seq    int64
	record *string
}

type KVServer struct {
	mu sync.Mutex

	database map[string]string
	// TODO:
	// we can remove old information by normal communicaing
	// but for abnormal exit?

	// golang has string interning, which means same string with use same pointer!

	historys map[int64]history
}

func (kv *KVServer) removeValueInHistory(cid int64, seq int64) {
	if h, ok := kv.historys[cid]; ok {
		if h.seq < seq {
			h.record = nil
			h.seq = 0
		}
	}
}

func (kv *KVServer) getValueFromHistory(cid int64, seq int64) *string {
	if h, ok := kv.historys[cid]; ok {
		if h.seq == seq {
			return h.record
		}
	}
	return nil
}

func (kv *KVServer) updateHistory(cid int64, seq int64, value *string) {
	kv.historys[cid] = history{seq, value}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// ------------------------test-----------------------

	// defer kv.memoryUsed()
	// defer fmt.Print(kv)

	// fmt.Printf("get: Cid=%v Seq=%v Ack=%v keylen=%v\n", args.Cid, args.Seq, args.Ack, len(args.Key))
	// ------------------------test-----------------------

	kv.removeValueInHistory(args.Cid, args.Seq)

	// Seems that return an newest value is ok

	// get data stored in database
	if v, ok := kv.database[args.Key]; ok {
		reply.Value = v
	}

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// ------------------------test-----------------------
	// defer kv.memoryUsed()
	// defer fmt.Print(kv)

	// fmt.Printf("put: Cid=%v Seq=%v Ack=%v key=%v valuelen=%v\n", args.Cid, args.Seq, args.Ack, args.Key, len(args.Value))
	// ------------------------test-----------------------

	if v := kv.getValueFromHistory(args.Cid, args.Seq); v != nil {
		reply.Value = *v
		return
	}

	// insert value into database
	kv.database[args.Key] = args.Value

	kv.updateHistory(args.Cid, args.Seq, &reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if v := kv.getValueFromHistory(args.Cid, args.Seq); v != nil {
		reply.Value = *v
		return
	}

	// update value in database
	old := kv.database[args.Key]
	kv.database[args.Key] = old + args.Value

	reply.Value = old

	kv.updateHistory(args.Cid, args.Seq, &old)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.database = make(map[string]string)

	kv.historys = make(map[int64]history)

	return kv
}
