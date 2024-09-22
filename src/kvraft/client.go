package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	leader  int
	id      int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.leader = 0

	ck.id = nrand()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	reply := GetReply{}

	args = GetArgs{clientRequestIdentity{ck.id, nrand()}, key}

	for {
		reply = GetReply{}
		ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if reply.Err == OK || reply.Err == ErrNoKey {
			break
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	if op != "Put" && op != "Append" {
		log.Fatalf("unknown op %v", op)
	}

	args := PutAppendArgs{clientRequestIdentity{ck.id, nrand()}, key, value}

	for {
		reply := PutAppendReply{}

		ck.servers[ck.leader].Call("KVServer."+op, &args, &reply)
		if reply.Err == OK {
			break
		}

		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
