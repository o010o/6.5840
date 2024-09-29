package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrWrongLeader    = "ErrWrongLeader"
	ErrUnknownArgs    = "ErrUnknownArgs"
	ErrConnectFailed  = "ErrConnectFailed"
	ErrHaveMigrated   = "ErrHaveMigrated"
	ErrConfigNotMatch = "ErrConfigNotMatch"
	ErrFallbackConfig = "ErrFallbackConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Id ClientRequestIdentity
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Id  ClientRequestIdentity
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	Id     ClientRequestIdentity
	Config shardConfig
	Data   migratedData
}

type MigrateReply struct {
	Err Err
}

type UpdateConfigArgs struct {
	Id     ClientRequestIdentity
	Config shardConfig
}

type UpdateConfigReply struct {
	Err Err
}

type DeleteArgs struct {
	Id  ClientRequestIdentity
	SId int
}

type DeleteReply struct {
	Err Err
}

type GetConfigArgs struct {
	SId int
}

type GetConfigReply struct {
	Config shardConfig
}
