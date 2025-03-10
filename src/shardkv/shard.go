package shardkv

import (
	"errors"
	"fmt"
	"sync"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

type shardState uint8

const (
	stateNotOwner shardState = 0
	stateOwner    shardState = 1
	stateTransfer shardState = 2
	stateMigrate  shardState = 3
)

var stateName map[shardState]string = map[shardState]string{
	stateNotOwner: "NotOwner",
	stateOwner:    "Owner",
	stateTransfer: "Transfer",
	stateMigrate:  "Migrate",
}

type shardConfig struct {
	SId int // shard num
	CId int // config num
	GId int // belong to which group

	// shard and current server.

	// Relation ship between shard and this server at this time. Owner, NotOwner or Transfer ownership.
	// Used for confirming ownership.
	// Should persister.
	// state each shard
	St shardState
}

func (sc *shardConfig) String() string {
	return fmt.Sprintf("sId=%v, cId=%v, gId=%v, st=%v", sc.SId, sc.CId, sc.GId, stateName[sc.St])
}

type shard struct {
	c [shardctrler.NShards]shardConfig

	mu         sync.Mutex
	threadCnts [shardctrler.NShards]int8
}

func (s *shard) construct() {
	for i := 0; i < len(s.c); i++ {
		s.c[i].SId = i
	}
}

func (s *shard) String() string {
	str := "\nsId\tcId\tgId\tst\n"
	for i := 0; i < len(s.c); i++ {
		str = str + fmt.Sprintf("%v\t%v\t%v\t%v\n", s.c[i].SId, s.c[i].CId, s.c[i].GId, stateName[s.c[i].St])
	}
	return str
}

func (s *shard) dupConfig(sId int) shardConfig {
	if sId < 0 || sId >= len(s.c) {
		panic(fmt.Sprintf("Invalid sId, sId=%v", sId))
	}

	return s.c[sId]
}

func (s *shard) isShardEnable(sId int) bool {
	if sId < 0 || sId >= len(s.c) {
		panic(fmt.Sprintf("Invalid sId, sId=%v", sId))
	}

	return s.c[sId].St == stateOwner
}

func isFromPeer(args *UpdateConfigArgs) bool {
	return args.Id != EmptyClientRequestIdentity
}

func (s *shard) doUpdateConfig(args *UpdateConfigArgs) (interface{}, error) {
	nc := &args.Config
	if isFromPeer(args) {
		c := s.dupConfig(nc.SId)
		if nc.CId > c.CId+1 {
			return nil, errors.New(ErrConfigNotMatch)
		} else if nc.CId < c.CId+1 {
			return nil, errors.New(ErrHaveMigrated)
		} else {
			if !(c.St == stateNotOwner && nc.St == stateOwner) {
				return nil, errors.New(ErrHaveMigrated)
			}
		}
	} else {
		if !s.isProgressConfig(nc) {
			return nil, errors.New(ErrFallbackConfig)
		}
	}

	s.c[nc.SId] = *nc

	return nil, nil
}

func (s *shard) serialization(e *labgob.LabEncoder) {
	e.Encode(s.c)
}

func (s *shard) unSerialization(d *labgob.LabDecoder) {
	tmp := [shardctrler.NShards]shardConfig{}
	if d.Decode(&tmp) != nil {
		panic("unSerialization")
	}
	copy(s.c[:], tmp[:])
}

func (s *shard) isProgressConfig(nc *shardConfig) bool {
	c := &s.c[nc.SId]
	if c.CId+1 == nc.CId {
		// NotOwner -> Owner/NotOwner, Owner -> Owner
		if c.St == stateNotOwner && (nc.St == stateOwner || nc.St == stateNotOwner) {
			// no migrate or MigrateFrom
			return true
		} else if c.St == stateOwner && nc.St == stateOwner {
			// no migrate
			return true
		} else if c.St == stateTransfer && nc.St == stateNotOwner {
			// MigrateTo
			return true
		} else {
			return false
		}
	} else if c.CId == nc.CId {
		// Owner -> Migrate -> Transfer
		if c.GId != nc.GId {
			panic(fmt.Sprintf("same config should have same gid, c={%v}", c))
		}
		if c.St == stateOwner && nc.St == stateMigrate {
			// MigrateTo
			return true
		} else if c.St == stateMigrate && nc.St == stateTransfer {
			// MigrateTo
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (s *shard) isProcessing(sId int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.threadCnts[sId] > 0
}

func (s *shard) markProcessing(sId int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.threadCnts[sId]++
}

func (s *shard) unmarkProcessing(sId int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.threadCnts[sId]--
}
