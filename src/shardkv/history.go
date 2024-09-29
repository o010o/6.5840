package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
)

type operationHistory struct {
	// h map[int64]int64
	// TODO: using aging algorithm to evict older history? However, it seems that there are still problems.
	// For example, a clerk may send the same request after a long interval. Aging algorithm may evict such history, but we should detect it.

	sr [shardctrler.NShards]map[int64]int64

	or map[int64]int64
}

func (oph *operationHistory) dupHistory(sid int) map[int64]int64 {
	c := make(map[int64]int64, 0)
	for cid, rid := range oph.sr[sid] {
		c[cid] = rid
	}
	return c
}

func (oph *operationHistory) migrate(sid int, nh *map[int64]int64) {
	oph.sr[sid] = make(map[int64]int64)

	for cid, rid := range *nh {
		oph.sr[sid][cid] = rid
	}
}

func (oph *operationHistory) construct() {
	for i := 0; i < len(oph.sr); i++ {
		oph.sr[i] = make(map[int64]int64)
	}

	oph.or = make(map[int64]int64)
}

func (oph *operationHistory) find(o *Op) bool {
	// For Write, it is no need to store any result for lab4. But we still need to store if write has been exexute
	// For read, it does not matter to re-execute it. So we dont store result either.
	switch o.T {
	case OpPutAppend:
		args := o.Args.(PutAppendArgs)
		_, ok := oph.sr[key2shard(args.Key)][args.Id.ClientId]
		if !ok {
			return false
		}
		return oph.sr[key2shard(args.Key)][args.Id.ClientId] == args.Id.RequestId
	case OpGet:
		args := o.Args.(GetArgs)
		_, ok := oph.sr[key2shard(args.Key)][args.Id.ClientId]
		if !ok {
			return false
		}
		return oph.sr[key2shard(args.Key)][args.Id.ClientId] == args.Id.RequestId
	case OpDelete:
		args := o.Args.(DeleteArgs)
		_, ok := oph.sr[args.SId][args.Id.ClientId]
		if !ok {
			return false
		}
		return oph.sr[args.SId][args.Id.ClientId] == args.Id.RequestId
	case OpUpdateConfig:
		args := o.Args.(UpdateConfigArgs)
		if args.Id == EmptyClientRequestIdentity {
			// update from inner of server, execute directly.
			return false
		}
		_, ok := oph.or[args.Id.ClientId]
		if !ok {
			return false
		}
		return oph.or[args.Id.ClientId] == args.Id.RequestId
	case OpMigrate:
		args := o.Args.(MigrateArgs)
		_, ok := oph.or[args.Id.ClientId]
		if !ok {
			return false
		}
		return oph.or[args.Id.ClientId] == args.Id.RequestId
	case OpGetConfig:
		return false
	default:
		panic("Do not support")
	}
}

func (oph *operationHistory) insert(o *Op, r *execResult) {
	switch o.T {
	case OpPutAppend:
		args := o.Args.(PutAppendArgs)
		oph.sr[key2shard(args.Key)][args.Id.ClientId] = args.Id.RequestId
	case OpGet:
		args := o.Args.(GetArgs)
		oph.sr[key2shard(args.Key)][args.Id.ClientId] = args.Id.RequestId
	case OpDelete:
		args := o.Args.(DeleteArgs)
		oph.sr[args.SId][args.Id.ClientId] = args.Id.RequestId
	case OpUpdateConfig:
		args := o.Args.(UpdateConfigArgs)
		oph.or[args.Id.ClientId] = args.Id.RequestId
	case OpMigrate:
		args := o.Args.(MigrateArgs)
		oph.or[args.Id.ClientId] = args.Id.RequestId
	case OpGetConfig:
		// It is not necessary to detect GetConfig
	default:
		panic("Do not support")
	}
}

func (oph *operationHistory) serialization(e *labgob.LabEncoder) {
	// TODO: Shall we compress history here?
	e.Encode(oph.sr)
	e.Encode(oph.or)
}

func (oph *operationHistory) unSerialization(d *labgob.LabDecoder) {
	sr := [shardctrler.NShards]map[int64]int64{}
	for i := 0; i < len(sr); i++ {
		sr[i] = make(map[int64]int64)
	}

	if d.Decode(&sr) != nil {
		panic("unSerialization history failed")
	}
	oph.sr = sr

	or := make(map[int64]int64)
	if d.Decode(&or) != nil {
		panic("unSerialization history failed")
	}

	oph.or = or
}
