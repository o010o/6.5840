package shardkv

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type MigrateDirectState uint8

const (
	Debug                                  = false
	CheckTermInterval   time.Duration      = 100 * time.Millisecond
	UpdateShardInterval time.Duration      = 100 * time.Millisecond
	MigrateDirectTo     MigrateDirectState = 1
	MigrateDirectFrom   MigrateDirectState = 2
	MigrateDirectNo     MigrateDirectState = 3
	MigrateDirectLoop   MigrateDirectState = 4
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientRequestIdentity struct {
	ClientId  int64
	RequestId int64
}

var (
	EmptyClientRequestIdentity = ClientRequestIdentity{-1, -1}
)

type execResult struct {
	err    Err
	result interface{}
}

type opMessage struct {
	op       *Op
	resultCh chan *execResult
}

type ShardKV struct {
	// This lock protect state machine which includes:
	// 1. ir
	// 2. opHistory
	// 3. database
	mu             sync.Mutex
	me             int
	rf             *raft.Raft
	dead           int32
	applyCh        chan raft.ApplyMsg
	make_end       func(string) *labrpc.ClientEnd
	gid            int
	ctrlers        []*labrpc.ClientEnd
	maxraftstate   int // snapshot if log grows this big
	newOpCh        chan *opMessage
	database       KVDatabase
	opHistory      operationHistory
	ctrlerLeaderId int32
	clientId       int64 // client id of shard controller
	cache          configsCache
	shard          shard
	// Record the index of last applied operation. Each index is coresponding to one state.
	ir indexRecord
}

func (kv *ShardKV) String() string {
	return fmt.Sprintf("me=%v, gid=%v", kv.me, kv.gid)
}

func (kv *ShardKV) updateConfig(sId int, nCid int, nGid int, nSt shardState) (interface{}, Err) {
	DPrintf("%v, updateConfig, newConfig={sId=%v, nCid=%v, nGid=%v, nSt=%v}", kv, sId, nCid, nGid, stateName[nSt])
	op := Op{OpUpdateConfig, UpdateConfigArgs{ClientRequestIdentity{kv.clientId, nrand()}, shardConfig{sId, nCid, nGid, nSt}}}
	return kv.execDispatch(&op)
}

func (kv *ShardKV) UpdateConfig(args *UpdateConfigArgs, reply *UpdateConfigReply) {
	DPrintf("%v, UpdateConfig, args={%v}", kv, args)

	// FIXME: A race condition occured when this server is migrating shard, and another server is re-transfer ownership to this server
	// No, I dont think re-transfer could reach here, because the configs are not matched.

	// TODO: So, the dup is not saft. Try to solve it.

	// Race if two leader(one is old the other is new) reach here.
	// This server may migrate shard after ole leader change the state
	// Then the new leader change the state again, so the state would be massed.

	// There may be other questions similar to the one above. I believe it is the architure that causes these problems.
	_, err := kv.updateConfig(args.Config.SId, args.Config.CId, args.Config.GId, args.Config.St)

	*reply = UpdateConfigReply{err}

	// FIXME: This server may start to execute procedure migrate() here
	// and the code may overwrite the disable in migrate(), which cause a migrated shard being served.
	// kv.updateAvailable()
}

type migratedData struct {
	Keys    []string
	Values  []string
	History map[int64]int64
}

func (kv *ShardKV) generateMigrateData(c *shardConfig) migratedData {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	md := migratedData{}
	// TODO: only dup the history of shard c.SID
	md.History = kv.opHistory.dupHistory(c.SId)
	md.Keys, md.Values = kv.database.fetchKVs(c.SId)

	return md
}

func (kv *ShardKV) sendShardTo(c *shardConfig, nc *shardctrler.Config) error {
	// TODO: Divide shard into multi messages and each k/v pair is wrapped into one Put operation. Then send to peer.
	// Divide advantage:
	// 1. Dont need to send all the pair, when need to re-migrate
	if c.St != stateMigrate {
		panic(fmt.Sprintf("%v, invalid state, c={%v}, nc={%v}", kv, c, nc))
	}

	// Send all pairs
	args := MigrateArgs{ClientRequestIdentity{kv.clientId, nrand()}, *c, kv.generateMigrateData(c)}

	if len(args.Data.Keys) == 0 {
		// No pairs to send
		return nil
	}

	serversName := nc.GetServers(nc.GetGid(c.SId))
	resultCh := make(chan bool, len(serversName))
	for _, serverName := range serversName {
		server := kv.make_end(serverName)
		if server == nil {
			continue
		}
		go func() {
			reply := MigrateReply{}
			ok := server.Call("ShardKV.MigrateKV", &args, &reply)
			if ok && reply.Err == OK {
				resultCh <- true
			} else {
				resultCh <- false
			}
		}()
	}

	for i := 0; i < len(serversName); i++ {
		res := <-resultCh
		if res {
			return nil
		}
	}

	return errors.New("send shard to peer failed")
}

func (kv *ShardKV) migrate(c *shardConfig, nc *shardctrler.Config) error {
	if c.GId == nc.GetGid(c.SId) {
		panic(fmt.Sprintf("me=%v, do not need to migrate k/v", kv.me))
	}

	var err error
	var errStr Err

	if c.St == stateOwner {
		goto PHASE1
	} else if c.St == stateMigrate {
		goto PHASE2
	} else if c.St == stateTransfer {
		goto PHASE3
	}

PHASE1:
	DPrintf("%v, migrate phase 1, disable shard, c={%v}", kv, c)
	_, errStr = kv.updateConfig(c.SId, c.CId, c.GId, stateMigrate)
	if errStr != OK {
		return errors.New("updateConfig failed")
	}
	c.St = stateMigrate

PHASE2:
	DPrintf("%v, migrate phase 2, send shard, c={%v}", kv, c)
	err = kv.sendShardTo(c, nc)
	if err != nil {
		return err
	}

PHASE3:
	DPrintf("%v, migrate phase 3, transfer ownership, c={%v}", kv, c)
	err = kv.transferOwnership(c, nc)
	if err != nil {
		return err
	}

	DPrintf("%v, migrate done, c={%v}", kv, c)

	return nil
}

func (kv *ShardKV) execDispatch(op *Op) (interface{}, Err) {
	resultCh := make(chan *execResult, 1)
	kv.newOpCh <- &opMessage{op, resultCh}

	res := <-resultCh

	return res.result, res.err
}

func (kv *ShardKV) MigrateKV(args *MigrateArgs, reply *MigrateReply) {
	// Svr1 migrate shard s1 to svr2. Cid1 is the config number of s1 in svr1, Cid2 is the config number of s1 in svr2.
	// Panic if:
	// - Cid1 < Cid2. Because the ownership is still holded by Svr1, so it is impossible that there is a server has higher cid of s1.
	// Go ahead if :
	// - Cid1 = Cid2. But it seems that Cid1 > Cid2 is ok.
	DPrintf("%v, MigrateKVRequest, args={%v}", kv, args)

	// Prevent newer shard from being influence by older shard.
	// kv.deleteShard(args.Config.SId)

	op := Op{OpMigrate, *args}
	_, err := kv.execDispatch(&op)

	*reply = MigrateReply{err}

	DPrintf("%v, MigrateKVReply, reply={%v}", kv, reply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("%v, PutAppendRequest, args={%v}", kv, args)

	if args.Op != "Put" && args.Op != "Append" {
		*reply = PutAppendReply{ErrUnknownArgs}
		return
	}

	op := Op{OpPutAppend, *args}

	_, err := kv.execDispatch(&op)

	*reply = PutAppendReply{err}

	DPrintf("%v, PutAppendReply, reply={%v}", kv, reply)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// handle operation one by one
	DPrintf("%v, GetRequest, args=%v", kv, args)

	op := Op{OpGet, *args}
	value, err := kv.execDispatch(&op)
	if err == OK {
		v, ok := value.(string)
		if !ok {
			panic("execDispatch return true with empty result")
		}
		*reply = GetReply{OK, v}
	} else {
		*reply = GetReply{err, ""}
	}

	// ---------- test---------------------
	DPrintf("%v, GetApply, reply={%v}", kv, reply)
}

func (kv *ShardKV) deleteShard(sId int) {
	op := Op{OpDelete, DeleteArgs{ClientRequestIdentity{kv.clientId, nrand()}, sId}}
	kv.execDispatch(&op)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) generateSnapshot() (int, []byte) {
	// Snapshot should contain state machine and historical request.
	// State machine should be saved because it will replace the removed log.
	// And we save historical requests because we want to verify whether the resumed state machine has the request identical to a new request.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	last := kv.ir.getLast()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	kv.database.serialization(e)
	kv.shard.serialization(e)
	kv.opHistory.serialization(e)

	return last, w.Bytes()
}

func (kv *ShardKV) applySnapshot(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.ir.setLast(msg.SnapshotIndex)

	r := bytes.NewBuffer(msg.Snapshot)
	d := labgob.NewDecoder(r)

	kv.database.unSerialization(d)
	kv.shard.unSerialization(d)
	kv.opHistory.unSerialization(d)

	DPrintf("%v, reset state machine, index=%v, term=%v, shard={%v}", kv, msg.SnapshotIndex, msg.SnapshotTerm, &kv.shard)
}

func (kv *ShardKV) applyCommand(index int, command interface{}) execResult {
	// execute command if the command is not repeated, or fetch result from history
	op, ok := command.(Op)
	if !ok {
		panic("not a valid command")
	}

	// Every operation which modify the state should not be permitted to execute multi times
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Sends response if write(Append(), Put()) has been executed. Re-executes read(Get()) request.
	if op.T != OpGet && kv.opHistory.find(&op) {
		return execResult{OK, nil}
	}

	// TODO: It seems that it is no need to execute Query when there is no listener
	// There are certain operations that can only be exeuted by the leader!!!
	// Like, query, and migrate

	kv.ir.advanceLast(index)

	// FIXME: This server should detect repeated request from migrating server.
	r := kv.applyOperation(index, &op)

	// Record operation which has been applied to prevent re-execution.
	if r.err == OK {
		kv.opHistory.insert(&op, &r)
	}

	return r
}

func (kv *ShardKV) execOpWorker() {
	nr := generateNotifyRecord()

	for !kv.killed() {
		select {
		case n := <-kv.newOpCh:
			index, term, isLeader := kv.rf.Start(*n.op)
			if !isLeader {
				n.resultCh <- &execResult{ErrWrongLeader, nil}
			} else {
				nr.push(term, index, n.resultCh)
			}
		case msg := <-kv.applyCh:
			if msg.CommandValid && msg.SnapshotValid {
				panic("executeOps: invalid apply message")
			}

			if msg.CommandValid {
				if kv.maxraftstate > 0 && kv.rf.RaftStateSize() > kv.maxraftstate {
					index, data := kv.generateSnapshot()
					DPrintf("%v, execute snapshot, index=%v, size=%v, max=%v", kv, index, kv.rf.RaftStateSize(), kv.maxraftstate)

					// FIXME: snapshot per operation, it is too frequent. I think it is the History that leads to this problem
					kv.rf.Snapshot(index, data)
				}

				res := kv.applyCommand(msg.CommandIndex, msg.Command)
				kv.tryNotify(nr, msg.CommandIndex, &res)
			} else if msg.SnapshotValid {
				kv.applySnapshot(&msg)
			}
		case <-time.After(CheckTermInterval):
			kv.tryNotify(nr, -1, nil)
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.cache.kill()
	// Your code here, if desired.
}

func (kv *ShardKV) applyOperation(index int, op *Op) execResult {
	DPrintf("%v, apply operation, index=%v, op={%v}", kv, index, op)

	switch op.T {
	case OpGet:
		args, ok := op.Args.(GetArgs)
		if !ok {
			panic("execOp")
		}

		if !kv.shard.isShardEnable(key2shard(args.Key)) {
			return execResult{ErrWrongGroup, nil}
		}

		value, err := kv.database.get(args.Key)
		if err != nil {
			return execResult{Err(err.Error()), nil}
		}
		return execResult{OK, value}
	case OpPutAppend:
		args, ok := op.Args.(PutAppendArgs)
		if !ok {
			panic("execOp")
		}

		// FIXME: operation happend between stateMigrate and NotOwner, will execute failed because of state.
		// when we try to re-execute it in another server, it will be regarded as repeated request, and will not be executed.
		if !kv.shard.isShardEnable(key2shard(args.Key)) {
			return execResult{ErrWrongGroup, nil}
		}

		if args.Op == "Put" {
			kv.database.put(args.Key, args.Value)
		} else if args.Op == "Append" {
			kv.database.append(args.Key, args.Value)
		} else {
			panic("Unknown operation")
		}
		return execResult{OK, nil}
	case OpDelete:
		args, ok := op.Args.(DeleteArgs)
		if !ok {
			panic("execOp")
		}

		kv.database.delete(args.SId)
		return execResult{OK, nil}
	case OpUpdateConfig:
		args, ok := op.Args.(UpdateConfigArgs)
		if !ok {
			panic("execOp")
		}

		// FIXME:
		_, err := kv.shard.doUpdateConfig(&args.Config)
		if err != nil {
			DPrintf("%v, update config failed, c={%v}, nc={%v}", kv, kv.shard.c[args.Config.SId], &args.Config)
			return execResult{Err(err.Error()), nil}
		}
		return execResult{OK, nil}
	case OpMigrate:
		args, ok := op.Args.(MigrateArgs)
		if !ok {
			panic("execOp")
		}

		c := kv.shard.dupConfig(args.Config.SId)
		if args.Config.CId > c.CId {
			return execResult{ErrConfigNotMatch, nil}
		} else if args.Config.CId < c.CId {
			// Re-migration is not permitted after the state of this server has been altered by a peer server
			panic(fmt.Sprintf("%v, shard had moved, c={%v}, nc={%v}", kv, &c, &args.Config))
		}

		kv.opHistory.migrate(args.Config.SId, &args.Data.History)
		kv.database.migrate(args.Config.SId, args.Data.Keys, args.Data.Values)
		return execResult{OK, nil}
	case OpGetConfig:
		args, ok := op.Args.(GetConfigArgs)
		if !ok {
			panic("execOp")
		}

		config := kv.shard.dupConfig(args.SId)
		return execResult{OK, config}
	default:
		panic("execOp: unknown operation")
	}
}

func (kv *ShardKV) isConfigChanged(c *shardConfig, nc *shardctrler.Config) bool {
	return nc.GetCid() > c.CId
}

func (kv *ShardKV) migrateDirect(c *shardConfig, nc *shardctrler.Config) MigrateDirectState {
	g1 := c.GId
	g2 := nc.GetGid(c.SId)

	if g1 == kv.gid && kv.gid != g2 {
		return MigrateDirectTo
	} else if g1 != kv.gid && kv.gid == g2 {
		return MigrateDirectFrom
	} else if g1 == kv.gid && kv.gid == g2 {
		return MigrateDirectLoop
	} else {
		return MigrateDirectNo
	}
}

func (kv *ShardKV) updateShard(sId int) {
	// FIXME: The whold work flow is complex, and does not work well.
	// There are so many error, and hard to analysis
	// Try to simplify the model.

	// Only one thread is permitted to process the shard.
	kv.shard.markProcessing(sId)
	defer kv.shard.unmarkProcessing(sId)

	c, err := kv.getConfig(sId)
	if err != nil {
		return
	}

	nc, err := kv.cache.get(c.CId + 1)
	if err != nil {
		DPrintf("%v, fetch config %v of shard %v failed, c={%v}", kv, c.CId+1, c.SId, c)
		return
	}

	if !kv.isConfigChanged(c, nc) {
		DPrintf("%v, config is not changed, c={%v}, nc={%v}", kv, c, nc)
		return
	}

	direct := kv.migrateDirect(c, nc)

	switch direct {
	case MigrateDirectFrom:
		// TODO: waiting until data and ownership reach?
		DPrintf("%v, MigrateFrom, c={%v}, nc={%v}", kv, c, nc)
		if c.GId == 0 {
			_, err := kv.updateConfig(c.SId, nc.GetCid(), nc.GetGid(c.SId), stateOwner)
			if err != OK {
				DPrintf("%v, update config failed, sId=%v, nc=%v, ng=%v, st=O", kv, c.SId, nc.GetCid(), nc.GetGid(c.SId))
			} else {
				DPrintf("%v, update config successfully, sId=%v, nc=%v, ng=%v, st=O", kv, c.SId, nc.GetCid(), nc.GetGid(c.SId))
			}
		}
	case MigrateDirectLoop:
		// Just update the configuration number if the ownership belongs to this server and remains unchanged.
		DPrintf("%v, MigrateLoop, c={%v}, nc={%v}", kv, c, nc)
		if c.St != stateOwner {
			panic("Invalid state")
		}
		kv.updateConfig(c.SId, nc.GetCid(), nc.GetGid(c.SId), stateOwner)
	case MigrateDirectTo:
		if nc.GetGid(c.SId) == 0 {
			panic("migrate shard to group 0, which could loss shard")
		}

		if c.St == stateNotOwner {
			panic("Invalid state")
		}

		// FIXME: New leader will re-execute migration after old leader had completed migration
		DPrintf("%v, MigrateTo, c={%v}, nc={%v}", kv, c, nc)
		kv.migrate(c, nc)
		// Send k/v pairs or transfer ownership according to the state.
	case MigrateDirectNo:
		// This server does not participate in the migration.
		if c.St != stateNotOwner {
			panic("Invalid state")
		}
		DPrintf("%v, MigrateNo, c={%v}, nc={%v}", kv, c, nc)
		// kv.deleteShard(c.SId)
		kv.updateConfig(c.SId, nc.GetCid(), nc.GetGid(c.SId), stateNotOwner)
	default:
		panic("unknown direct")
	}

	// s
	// Gid is the gourp id of current server.
	// assert c.cid = nc.cid

	// c.gid == gid:
	// 		nc.gid = c.gid -> c <- nc
	// 		st(s) = O, do migrate.
	// 		st(s) = T, do transfer ownership.
	// 		st(s) = N, panic.

	// c.gid != gid:
	// assert st(s) != O, T
	// c <- nc

	// FIXME: dead lock?
	// Case that may cause race:
	// shard  1     2
	// group  1     2

	// shard  1     2
	// group  2     1

	// group 1 migrate k/v to group 2, and group 2 migrate k/v to group 1, may dead lock.

	// this algorithm request:
	// 1. Persist config.
	// 2. Let the leader control the migrate
	// 3. Disable shard immediately when a group loss shard.
	// 4. Move shard in the sequence of config
	// 5. Migreation could be repeated, so the server should do repeated detect.
	// 6. Migreate only the two leader have same config number.
}

// func (kv *ShardKV) updateAvailable() {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	for i := 0; i < len(kv.shard.availables); i++ {
// 		if kv.shard.c[i].st == stateOwner {
// 			if kv.shard.c[i].gId != kv.gid {
// 				panic(fmt.Sprintf("Invalid state of shard, shard={%v}", kv.shard))
// 			}
// 			kv.shard.availables[i] = true
// 		}
// 	}
// }

func (kv *ShardKV) transferOwnership(c *shardConfig, nc *shardctrler.Config) error {
	// Work flow of transfer ownership
	//   	  Svr1                		Svr2
	//1.      st(s1) = T
	//2.                          		st(s1) = O, g(s1) = Svr2
	//        st(s1) = N,g(s1) = Svr2
	// start at 1 if st(s1) = O, 2 if st(s1) = T
	if c.GId != kv.gid {
		panic(fmt.Sprintf("me=%v,current server does not own the ownership of the shard, gid=%v, c={%v}", kv.me, kv.gid, c))
	}

	if !kv.isConfigChanged(c, nc) {
		panic(fmt.Sprintf("can not move shard from c to nc, c=%v, nc=%v", c, nc))
	}

	args := UpdateConfigArgs{ClientRequestIdentity{kv.clientId, nrand()}, shardConfig{c.SId, nc.GetCid(), nc.GetGid(c.SId), stateOwner}}
	var err Err

	if c.St == stateMigrate {
		goto PHASE1
	} else if c.St == stateTransfer {
		goto PHASE2
	} else {
		panic(fmt.Sprintf("transferOwnership: invalid config state, c={%v}", c))
	}

PHASE1:
	DPrintf("%v, transferOwnership phase 1, mark transfer, c={%v}", kv, c)
	_, err = kv.updateConfig(c.SId, c.CId, c.GId, stateTransfer)
	if err != OK {
		DPrintf("%v, updateConfig failed at phase 1 in transferOwnership, c={%v}, nc={%v}", kv, c, nc)
		return errors.New(string(err))
	}

PHASE2:
	DPrintf("%v, transferOwnership phase 2, update state of peer, c={%v}", kv, c)
	serversName := nc.GetServers(nc.GetGid(c.SId))
	resultCh := make(chan bool, len(serversName))
	for _, serverName := range serversName {
		go func(name string) {
			reply := UpdateConfigReply{}
			server := kv.make_end(name)
			if server == nil {
				return
			}
			ok := server.Call("ShardKV.UpdateConfig", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrHaveMigrated) {
				resultCh <- true
			} else {
				resultCh <- false
			}
		}(serverName)
	}

	exit := true
	for i := 0; i < len(serversName); i++ {
		res := <-resultCh
		if res {
			exit = false
			break
		}
	}

	if exit {
		DPrintf("%v, transferOwnership phase 2 failed, c={%v}", kv, c)
		return errors.New("update config of peer server failed")
	}

	DPrintf("%v, transferOwnership phase 3, mark not owner, c={%v}", kv, c)
	_, err = kv.updateConfig(c.SId, nc.GetCid(), nc.GetGid(c.SId), stateNotOwner)
	if err != OK {
		DPrintf("%v, updateConfig failed at phase 3 in transferOwnership, c={%v}, nc={%v}", kv, c, nc)
		return errors.New(string(err))
	}

	return nil
}

func (kv *ShardKV) getConfig(sId int) (*shardConfig, error) {
	args := GetConfigArgs{SId: sId}
	op := Op{OpGetConfig, args}

	v, err := kv.execDispatch(&op)
	if err == OK {
		c, ok := v.(shardConfig)
		if !ok {
			panic("trans result to shardconfig failed")
		}
		return &c, nil
	}
	return nil, errors.New(string(err))
}

func (kv *ShardKV) updateConfigWorker() {

	for !kv.killed() {
		// XXX:This code is Bullshit
		_, isLeader := kv.rf.GetState()
		if isLeader {
			// FIXME: may not be a leader at this moment
			// TODO: How to ensure that only the leader update shard.
			for sId := 0; sId < len(kv.shard.c); sId++ {
				if kv.shard.isProcessing(sId) {
					continue
				}
				go kv.updateShard(sId)
				// // FIXME: There are some commited entries may not be applied here, so the state of shard may be stale.
				// // c := kv.shard.dupConfig(i)
				// c := kv.getConfig(i)
				// isPro := kv.shard.isProcessing(c.SId)
				// if isPro {
				// 	continue
				// }

				// go kv.updateShard(&c)
			}
		}

		time.Sleep(UpdateShardInterval)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(DeleteArgs{})
	labgob.Register(UpdateConfigArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	labgob.Register(MigrateArgs{})
	labgob.Register(GetConfigArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	//----my initialization code---
	kv.newOpCh = make(chan *opMessage, 1)
	kv.database.construct()
	kv.opHistory.construct()
	kv.ctrlerLeaderId = 0
	kv.clientId = nrand()
	kv.cache.construct(ctrlers)
	kv.shard.construct()
	// TODO: labgob may need to register more struct.
	//--------------------

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.execOpWorker()
	go kv.updateConfigWorker()

	return kv
}
