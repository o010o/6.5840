package kvsrv

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	id     int64
	seq    int64

	// TODO:
	// 1. add response state to make server delete some history ?
	// recored X, which all response below X had been seen by client.
}

// update statistic of successful request

func (c *Clerk) getSeq() int64 {
	o := c.seq
	c.seq = c.seq + 1
	return o
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

	args := GetArgs{ck.id, ck.getSeq(), key}
	reply := GetReply{}

	for {
		if ck.server.Call("KVServer.Get", &args, &reply) {
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

	args := PutAppendArgs{ck.id, ck.getSeq(), key, value}
	reply := PutAppendReply{}

	for {
		if ck.server.Call("KVServer."+op, &args, &reply) {
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
