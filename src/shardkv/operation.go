package shardkv

import (
	"fmt"
)

type OpType int

const (
	OpGet          OpType = 1
	OpPutAppend    OpType = 2
	OpUpdateConfig OpType = 4
	OpMigrate      OpType = 5
	OpGetConfig    OpType = 6
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	T    OpType
	Args interface{}
}

func (o *Op) getIdentity() ClientRequestIdentity {
	switch o.T {
	case OpPutAppend:
		args := o.Args.(PutAppendArgs)
		return args.Id
	case OpGet:
		args := o.Args.(GetArgs)
		return args.Id
	case OpUpdateConfig:
		args := o.Args.(UpdateConfigArgs)
		return args.Id
	case OpMigrate:
		args := o.Args.(MigrateArgs)
		return args.Id
	case OpGetConfig:
		return EmptyClientRequestIdentity
	default:
		panic("Do not support")
	}
}

func (o *Op) String() string {
	var opsName map[OpType]string = map[OpType]string{
		OpGet:          "Get",
		OpPutAppend:    "PutAppend",
		OpUpdateConfig: "UpdateConfig",
		OpMigrate:      "Migrate",
		OpGetConfig:    "GetConfig",
	}

	str := "op=" + opsName[o.T] + ", "

	switch o.T {
	case OpPutAppend:
		args := o.Args.(PutAppendArgs)
		str = str + fmt.Sprintf("args={%v}", &args)
	case OpGet:
		args := o.Args.(GetArgs)
		str = str + fmt.Sprintf("args={%v}", &args)
	case OpUpdateConfig:
		args := o.Args.(UpdateConfigArgs)
		str = str + fmt.Sprintf("args={%v}", &args)
	case OpMigrate:
		args := o.Args.(MigrateArgs)
		str = str + fmt.Sprintf("args={%v}", &args)
	case OpGetConfig:
		args := o.Args.(GetConfigArgs)
		str = str + fmt.Sprintf("args={%v}", &args)
	default:
		panic("Do not support")
	}

	return str
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("Id=%v, K=%v, V=%v, sId=%v, Op={%v}", args.Id, args.Key, args.Value, key2shard(args.Key), args.Op)
}

func (args *UpdateConfigArgs) String() string {
	return fmt.Sprintf("Id=%v, c={%v}", args.Id, &args.Config)
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("Id=%v, K=%v, sId=%v", args.Id, args.Key, key2shard(args.Key))
}

func (args *MigrateArgs) String() string {
	return fmt.Sprintf("Id=%v, c={%v}, OpLen=%v", args.Id, &args.Config, len(args.Data.Keys))
}

func (args *GetConfigArgs) String() string {
	return fmt.Sprintf("sId=%v", args.SId)
}
