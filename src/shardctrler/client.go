package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader   int
	clientId int64
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

	ck.leader = 0
	ck.clientId = nrand()

	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.Id = ClientRequestIdentity{ck.clientId, nrand()}
	for {
		// try each known server.
		for times := 0; times < len(ck.servers); times++ {
			srv := ck.servers[ck.leader]
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				return reply.Config
			}

			ck.leader = (ck.leader + 1) % len(ck.servers)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.Id = ClientRequestIdentity{ck.clientId, nrand()}
	for {
		// try each known server.
		for times := 0; times < len(ck.servers); times++ {
			srv := ck.servers[ck.leader]
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.Id = ClientRequestIdentity{ck.clientId, nrand()}
	for {
		// try each known server.
		for times := 0; times < len(ck.servers); times++ {
			srv := ck.servers[ck.leader]
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Id = ClientRequestIdentity{ck.clientId, nrand()}
	for {
		// try each known server.
		for times := 0; times < len(ck.servers); times++ {
			var reply MoveReply
			ok := ck.servers[ck.leader].Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}

			ck.leader = (ck.leader + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
