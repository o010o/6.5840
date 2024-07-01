package kvsrv

import (
	"fmt"
	"log"
	_ "net/http/pprof"
	"runtime"
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
	records   []*string
	oldestSeq int64
}

func (h *history) String() string {
	str := ""
	str = fmt.Sprintf("oldestSeq=%v, recordsLen=[", h.oldestSeq)
	for _, s := range h.records {
		if s == nil {
			str += "nil, "
			continue
		}
		str += fmt.Sprintf("%v, ", len(*s))
	}

	str = str[:len(str)-2]
	str += "]"

	return str
}

// add a new record to histroy
func (h *history) add(seq int64, record *string) {
	index := seq - h.oldestSeq
	for len(h.records) <= int(index) {
		h.records = append(h.records, nil)
	}

	h.records[index] = record
}

// remove record whose id <= ack
func (h *history) remove(ack int64) {
	if ack <= h.oldestSeq || len(h.records) == 0 {
		return
	}

	len := ack - h.oldestSeq

	h.records = h.records[len:]
	h.oldestSeq = ack
}

// get record who has seq
func (h *history) get(seq int64) *string {
	if seq < h.oldestSeq || seq-h.oldestSeq >= int64(len(h.records)) {
		return nil
	}
	return h.records[seq-h.oldestSeq]
}

type KVServer struct {
	mu sync.Mutex

	database map[string]string
	// TODO:
	// we can remove old information by normal communicaing
	// but for abnormal exit?

	// golang has string interning, which means same string with use same pointer!

	historys map[int64]*history
}

func (kvs *KVServer) memoryUsed() {
	runtime.GC()

	var st runtime.MemStats
	const (
		MiB = 1 << 20
	)
	runtime.ReadMemStats(&st)
	m := st.HeapAlloc / MiB
	fmt.Printf("memory used=%v\n", m)
}

func (kvs *KVServer) String() string {
	var str string

	for k, h := range kvs.historys {
		if h == nil {
			str += "nil\n"
			continue
		}
		str += fmt.Sprintf("cid=%v ", k)
		str += fmt.Sprintf("%v\n", h)
	}

	return str
}

func (kv *KVServer) removeValueInHistory(cid int64, seq int64, ack int64) {
	if _, ok := kv.historys[cid]; ok {
		kv.historys[cid].remove(ack)
	}
}

func (kv *KVServer) getValueFromHistory(cid int64, seq int64) *string {
	if _, ok := kv.historys[cid]; ok {
		return kv.historys[cid].get(seq)
	}
	return nil
}

func (kv *KVServer) addValueToHistoy(cid int64, seq int64, value *string) {
	if _, ok := kv.historys[cid]; !ok {
		kv.historys[cid] = new(history)
	}

	kv.historys[cid].add(seq, value)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// ------------------------test-----------------------

	// defer kv.memoryUsed()
	// defer fmt.Print(kv)

	// fmt.Printf("get: Cid=%v Seq=%v Ack=%v keylen=%v\n", args.Cid, args.Seq, args.Ack, len(args.Key))
	// ------------------------test-----------------------

	kv.removeValueInHistory(args.Cid, args.Seq, args.Ack)

	// if v := kv.getValueFromHistory(args.Cid, args.Seq); v != nil {
	// 	reply.Value = *v
	// 	return
	// }

	// Seems that return an newest value is ok

	// get data stored in database
	if v, ok := kv.database[args.Key]; ok {
		reply.Value = v
	}

	// kv.addValueToHistoy(args.Cid, args.Seq, &reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// ------------------------test-----------------------
	// defer kv.memoryUsed()
	// defer fmt.Print(kv)

	// fmt.Printf("put: Cid=%v Seq=%v Ack=%v key=%v valuelen=%v\n", args.Cid, args.Seq, args.Ack, args.Key, len(args.Value))
	// ------------------------test-----------------------

	kv.removeValueInHistory(args.Cid, args.Seq, args.Ack)

	if v := kv.getValueFromHistory(args.Cid, args.Seq); v != nil {
		reply.Value = *v
		return
	}

	// insert value into database
	kv.database[args.Key] = args.Value

	kv.addValueToHistoy(args.Cid, args.Seq, &reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.removeValueInHistory(args.Cid, args.Seq, args.Ack)

	if v := kv.getValueFromHistory(args.Cid, args.Seq); v != nil {
		reply.Value = *v
		return
	}

	// update value into database
	old := kv.database[args.Key]
	reply.Value = old
	kv.database[args.Key] = old + args.Value

	kv.addValueToHistoy(args.Cid, args.Seq, &reply.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.database = make(map[string]string)

	kv.historys = make(map[int64]*history)
	/*
		go func() {
			log.Println(http.ListenAndServe("0.0.0.0:10000", nil))
		}()
	*/

	return kv
}
