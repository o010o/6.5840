package shardkv

import (
	"errors"
	"sync"
	"sync/atomic"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

type confCache struct {
	mu sync.Mutex
	c  *shardctrler.Config
}

type configsCache struct {
	mu       sync.Mutex
	clientId int64
	leaderId int32
	dead     int32
	ctrlers  []*labrpc.ClientEnd
	configs  map[int]*confCache
}

func (cc *configsCache) construct(ctrlers []*labrpc.ClientEnd) {
	cc.clientId = nrand()
	cc.leaderId = 0
	cc.dead = 0
	cc.ctrlers = ctrlers
	cc.configs = make(map[int]*confCache)
}

func (cc *configsCache) kill() {
	atomic.StoreInt32(&cc.dead, 1)
}

func (cc *configsCache) killed() bool {
	return atomic.LoadInt32(&cc.dead) == 1
}

func (cc *configsCache) fetchConfig(cid int) (*shardctrler.Config, error) {
	args := &shardctrler.QueryArgs{}
	args.Num = cid
	args.Id = shardctrler.ClientRequestIdentity{ClientId: cc.clientId, RequestId: nrand()}
	for !cc.killed() {
		// Try stored leader server
		leaderId := atomic.LoadInt32(&cc.leaderId)
		var reply shardctrler.QueryReply
		ok := cc.ctrlers[leaderId].Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader {
			return &reply.Config, nil
		}

		// Try other server parallel
		res := make(chan *shardctrler.Config, len(cc.ctrlers))
		for i := 0; i < len(cc.ctrlers); i++ {
			if i == int(leaderId) {
				continue
			}

			go func(serverId int) {
				var reply shardctrler.QueryReply
				ok := cc.ctrlers[serverId].Call("ShardCtrler.Query", args, &reply)
				if ok && !reply.WrongLeader {
					atomic.StoreInt32(&cc.leaderId, int32(serverId))
					res <- &reply.Config
				} else {
					res <- nil
				}
			}(i)
		}

		for i := 0; i < len(cc.ctrlers); i++ {
			if i == int(leaderId) {
				continue
			}
			r := <-res
			if r != nil {
				return r, nil
			}
		}
	}
	return nil, errors.New("server has been killed")
}

func (cc *configsCache) getConfigCache(cid int) *confCache {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	_, ok := cc.configs[cid]
	if !ok {
		cc.configs[cid] = &confCache{sync.Mutex{}, nil}
	}
	return cc.configs[cid]
}

func (cc *configsCache) get(cId int) (*shardctrler.Config, error) {
	configCache := cc.getConfigCache(cId)
	configCache.mu.Lock()
	defer configCache.mu.Unlock()

	if configCache.c == nil {
		nc, err := cc.fetchConfig(cId)
		if err != nil {
			return nil, err
		}
		if nc.GetCid() == cId {
			configCache.c = nc
		} else {
			return nil, errors.New("fetch configuration failed")
		}
	}
	return configCache.c, nil
}

func (cc *configsCache) removeBelow(cId int) {
	// remove configuration whose num is smaller than the smallest config num of shards.
	cc.mu.Lock()
	defer cc.mu.Unlock()

	for id := range cc.configs {
		if id < cId {
			delete(cc.configs, id)
		}
	}
}
