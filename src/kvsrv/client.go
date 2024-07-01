package kvsrv

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	id     int64
	seq    int64

	// TODO:
	// 1. add response state to make server delete some history ?
	// recored X, which all response below X had been seen by client.
	mu       sync.Mutex
	succReqs map[int64]bool
	ack      int64 // default value is seq. If seq of request equal ack, then ack is invalid
}

// update statistic of successful request
func (c *Clerk) updateReq(seq int64) {
	if seq < c.ack {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.ack = seq + 1

	// c.succReqs[seq] = true
	// nAck := c.ack
	// for {
	// 	if _, ok := c.succReqs[nAck]; !ok {
	// 		break
	// 	}
	// 	delete(c.succReqs, nAck)
	// 	nAck++
	// }

	// c.ack = nAck
}

func (c *Clerk) getAck() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ack
}

func (c *Clerk) getSeq() int64 {
	return atomic.AddInt64(&c.seq, 1) - 1
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server

	ck.seq = 0

	ck.id = nrand()

	ck.succReqs = make(map[int64]bool)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	args := GetArgs{ck.id, ck.getSeq(), ck.getAck(), key}
	reply := GetReply{}

	for {
		if ck.server.Call("KVServer.Get", &args, &reply) {
			ck.updateReq(args.Seq)
			break
		}
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	if op != "Put" && op != "Append" {
		log.Fatalf("unknown op %v", op)
	}

	args := PutAppendArgs{ck.id, ck.getSeq(), ck.getAck(), key, value}
	reply := PutAppendReply{}

	for {
		if ck.server.Call("KVServer."+op, &args, &reply) {
			ck.updateReq(args.Seq)
			break
		}
	}

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
