package kvsrv

// Put or Append
type PutAppendArgs struct {
	Cid   int64
	Seq   int64
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Cid int64
	Seq int64
	Key string

	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
}
