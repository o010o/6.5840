package shardctrler

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32
	newOpCh  chan opMessage
	resultCh chan execResult
	history  operationHistory

	// Your data here.

	configs []Config // indexed by config num
}

type OpType int

const (
	Join              OpType        = 0
	Leave             OpType        = 1
	Move              OpType        = 2
	Query             OpType        = 3
	CheckTermInterval time.Duration = 100 * time.Millisecond
	Debug             bool          = true
)

var (
	ErrWrongLeader = errors.New("ErrWrongLeader")
	ErrRepeatedKey = errors.New("ErrRepeatedKey")
	EmptyNotify    = notify{nil, -1, -1, nil}
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your data here.
	T    OpType
	Args interface{}
}

func (o *Op) getId() clientRequestIdentity {
	switch o.T {
	case Join:
		args := o.Args.(*JoinArgs)
		return args.Id
	case Leave:
		args := o.Args.(*LeaveArgs)
		return args.Id
	case Move:
		args := o.Args.(*MoveArgs)
		return args.Id
	case Query:
		args := o.Args.(*QueryArgs)
		return args.Id
	}
	return clientRequestIdentity{-1, -1}
}

func (sc *ShardCtrler) tryNotify(msg *raft.ApplyMsg, result *execResult, no *notify) {
	if no == nil || *no != EmptyNotify || result == nil || no.isEmpty() {
		return
	}
	// notify waiting thread if
	// 1. term changed
	// 2. executing operation done
	term, isLeader := sc.rf.GetState()
	if term != no.term {
		no.ch <- &execResult{ErrWrongLeader, nil}
		no.reset()
		return
	}

	if msg == nil {
		return
	}

	if no.index == msg.CommandIndex {
		if !isLeader {
			panic(fmt.Sprintf("executeOps: why server is not leader at term %v", term))
		}

		no.ch <- result
		no.reset()
		return
	} else if no.index < msg.CommandIndex {
		panic("lost notify")
	}
}

func (sc *ShardCtrler) handleCommand(msg *raft.ApplyMsg) execResult {
	// execute command if the command is not repeated, or fetch result from history
	op, ok := msg.Command.(Op)
	if !ok {
		panic("not a valid command")
	}

	// re-execute query because we dont record the execute result
	if sc.history.find(&op) && op.T != Query {
		return execResult{nil, nil}
	}

	r := sc.execOp(&op)

	sc.history.insert(&op, &r)

	return r
}

func (sc *ShardCtrler) balanceServer(c *Config) {
	// Balance requests:
	// 1. Divide the shards into group as evenly as possible
	// 2. Move shard as less as possible

	averageShard := 0                                 // count of shard that each group should possess
	shardsEachGroup := make([][]int, len(c.Groups)+1) // number of shard that each group possess
	for i := 0; i < len(shardsEachGroup); i++ {
		shardsEachGroup[i] = make([]int, 0)
	}

	// return if no group
	if len(c.Groups) == 0 {
		return
	}

	// get average shard of each group
	averageShard = NShards / len(c.Groups)

	// generate statistic on the number of shards each group possesses
	for shard, gid := range c.Shards {
		shardsEachGroup[gid] = append(shardsEachGroup[gid], shard)
	}

	// move shard from group which have more shard over than average to group which has less shard
	// some move request:
	// 1. move shard which has bigger num first
	// 2. move all shard of group 0

	fromGroup := make([]int, 0) // These group have extra shard which can move to another group
	toGroup := make([]int, 0)   // These group need more shard

	// init fromGroup and toGroup according to averageShard
	fromGroup = append(fromGroup, 0)
	for i := 1; i < len(shardsEachGroup); i++ {
		if len(shardsEachGroup[i]) > averageShard {
			fromGroup = append(fromGroup, i)
		} else if len(shardsEachGroup[i]) < averageShard {
			toGroup = append(toGroup, i)
		}
	}

	// move shard from fromGroup to toGroup
	for len(toGroup) > 0 && len(fromGroup) > 0 {
		toNum := toGroup[0]
		fromNum := fromGroup[0]

		for len(shardsEachGroup[toNum]) < averageShard {
			shardsEachGroup[toNum] = append(shardsEachGroup[toNum], shardsEachGroup[fromNum][len(shardsEachGroup)-1])
			shardsEachGroup[fromNum] = shardsEachGroup[fromNum][0 : len(shardsEachGroup)-1]
			if len(shardsEachGroup[fromNum]) <= averageShard {
				fromGroup = fromGroup[1:]
				break
			}
		}

		if len(shardsEachGroup[toNum]) >= averageShard {
			toGroup = toGroup[1:]
		}
	}

	newShard := c.Shards
	for group, shards := range shardsEachGroup {
		for _, shard := range shards {
			newShard[shard] = group
		}
	}

	c.Shards = newShard
}

func (sc *ShardCtrler) doJoin(args *JoinArgs) execResult {
	newConfig := sc.configs[len(sc.configs)-1].dup()

	// add new gid->server to new config
	for gid, servers := range args.Servers {
		_, ok := newConfig.Groups[gid]
		if ok {
			// GID has been used
			return execResult{ErrRepeatedKey, nil}
		}

		newConfig.Groups[gid] = make([]string, len(servers))

		copy(newConfig.Groups[gid], servers)
	}

	sc.balanceServer(newConfig)

	sc.addNewConfig(newConfig)

	return execResult{nil, nil}
}

func (sc *ShardCtrler) doLeave(args *LeaveArgs) execResult {
	newConfig := sc.configs[len(sc.configs)-1].dup()

	for gid := range args.GIDs {
		// delete map between shard and group, then delete group
		for i, g := range newConfig.Shards {
			if g == gid {
				newConfig.Shards[i] = 0
			}
		}
		delete(newConfig.Groups, gid)
	}

	sc.balanceServer(newConfig)

	sc.addNewConfig(newConfig)

	return execResult{nil, nil}
}

func (sc *ShardCtrler) doMove(args *MoveArgs) execResult {
	newConfig := sc.configs[len(sc.configs)-1].dup()

	newConfig.Shards[args.Shard] = args.GID

	sc.addNewConfig(newConfig)

	return execResult{nil, nil}
}

func (sc *ShardCtrler) doQuery(args *QueryArgs) execResult {
	if args.Num == -1 || args.Num > sc.configs[len(sc.configs)-1].Num {
		return execResult{nil, &sc.configs[len(sc.configs)-1]}
	}

	return execResult{nil, &sc.configs[args.Num]}
}

func (sc *ShardCtrler) execOp(op *Op) execResult {
	switch op.T {
	case Join:
		args, ok := op.Args.(*JoinArgs)
		if !ok {
			panic("can not get JoinArgs")
		}
		return sc.doJoin(args)
	case Leave:
		args, ok := op.Args.(*LeaveArgs)
		if !ok {
			panic("can not get LeaveArgs")
		}
		return sc.doLeave(args)
	case Move:
		args, ok := op.Args.(*MoveArgs)
		if !ok {
			panic("can not get MoveArgs")
		}
		return sc.doMove(args)
	case Query:
		args, ok := op.Args.(*QueryArgs)
		if !ok {
			panic("can not get QueryArgs")
		}
		return sc.doQuery(args)
	}

	return execResult{errors.New("unknown operation"), nil}
}

type execResult struct {
	Err    error
	result interface{}
}

type opMessage struct {
	op       *Op
	resultCh chan *execResult
}

type notify struct {
	op    *Op
	index int
	term  int
	ch    chan *execResult
}

func (no *notify) init(op *Op, index int, term int, ch chan *execResult) {
	no.op = op
	no.index = index
	no.term = term
	no.ch = ch
}

func (no *notify) reset() {
	*no = EmptyNotify
}

func (no *notify) isEmpty() bool {
	return *no == EmptyNotify
}

func (sc *ShardCtrler) worker() {
	no := EmptyNotify

	for !sc.killed() {
		select {
		case n := <-sc.newOpCh:
			// spread operation and init notify

			index, term, isLeader := sc.rf.Start(n.op)
			if !isLeader {
				n.resultCh <- &execResult{ErrWrongLeader, nil}
			} else {
				no = notify{n.op, index, term, n.resultCh}
			}
		case msg := <-sc.applyCh:
			// Term may change here, so does leader. Then what happened. Which aspect?
			if msg.CommandValid && msg.SnapshotValid {
				panic("executeOps: invalid apply message")
			}

			if msg.CommandValid {
				res := sc.handleCommand(&msg)
				// report execute result if notify was setted.
				sc.tryNotify(&msg, &res, &no)
			} else if msg.SnapshotValid {
				DPrintf("me=%v, do not support snapshot", sc.me)
			}
		case <-time.After(CheckTermInterval):
			sc.tryNotify(nil, &execResult{ErrWrongLeader, nil}, &no)
		}
	}
}

func (sc *ShardCtrler) execDispatch(op *Op) (interface{}, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	resultCh := make(chan *execResult, 1)
	sc.newOpCh <- opMessage{op, resultCh}

	// wait until executing operation done
	res := <-resultCh

	return res.result, res.Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	_, err := sc.execDispatch(&Op{Join, args})
	if err != nil {
		*reply = JoinReply{true, Err(err.Error())}
	} else {
		*reply = JoinReply{false, OK}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	_, err := sc.execDispatch(&Op{Leave, args})
	if err != nil {
		*reply = LeaveReply{true, Err(err.Error())}
	} else {
		*reply = LeaveReply{false, OK}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	_, err := sc.execDispatch(&Op{Move, args})
	if err != nil {
		*reply = MoveReply{true, Err(err.Error())}
	} else {
		*reply = MoveReply{false, OK}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	res, err := sc.execDispatch(&Op{Move, args})
	if err != nil {
		*reply = QueryReply{true, Err(err.Error()), Config{}}
	} else {
		r, ok := res.(Config)
		if !ok {
			panic("execute successfully, but dont get result")
		}
		*reply = QueryReply{false, OK, r}
	}
}

func (sc *ShardCtrler) addNewConfig(c *Config) {
	c.Num = sc.configs[len(sc.configs)-1].Num + 1
	sc.configs = append(sc.configs, *c)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead, 1)
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.newOpCh = make(chan opMessage, 1)
	sc.resultCh = make(chan execResult, 1)
	sc.history.construct()

	// Your code here.
	go sc.worker()

	return sc
}

func (c *Config) dup() *Config {
	newConfig := Config{}

	newConfig.Num = c.Num
	// asign of array is deep copy
	newConfig.Shards = c.Shards

	newConfig.Groups = make(map[int][]string)
	for k, v := range c.Groups {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
	}

	return &newConfig
}
