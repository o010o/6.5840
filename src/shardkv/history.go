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
}

func (oph *operationHistory) find(o *Op) bool {
	// For Write, it is no need to store any result for lab4. But we still need to store if write has been exexute
	// For read, it does not matter to re-execute it. So we dont store result either.
	switch o.T {
	case OpPutAppend:
		args := o.Args.(PutAppendArgs)
		rid, ok := oph.sr[key2shard(args.Key)][args.Id.ClientId]
		if !ok {
			return false
		}
		return rid == args.Id.RequestId
	case OpUpdateConfig:
		args := o.Args.(UpdateConfigArgs)
		if !isFromPeer(&args) {
			return false
		}
		rid, ok := oph.sr[args.Config.SId][args.Id.ClientId]
		if !ok {
			return false
		}
		return rid == args.Id.RequestId
	case OpMigrate:
		args := o.Args.(MigrateArgs)
		rid, ok := oph.sr[args.Config.SId][args.Id.ClientId]
		if !ok {
			return false
		}
		return rid == args.Id.RequestId
	default:
		return false
	}
}

func (oph *operationHistory) insert(o *Op, r *execResult) {
	switch o.T {
	case OpPutAppend:
		args := o.Args.(PutAppendArgs)
		oph.sr[key2shard(args.Key)][args.Id.ClientId] = args.Id.RequestId
	case OpUpdateConfig:
		args := o.Args.(UpdateConfigArgs)
		if !isFromPeer(&args) {
			return
		}

		oph.sr[args.Config.SId][args.Id.ClientId] = args.Id.RequestId
	case OpMigrate:
		args := o.Args.(MigrateArgs)
		oph.sr[args.Config.SId][args.Id.ClientId] = args.Id.RequestId
	default:
	}
}

func (oph *operationHistory) serialization(e *labgob.LabEncoder) {
	e.Encode(oph.sr)
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
}
