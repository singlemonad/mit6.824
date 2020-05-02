package shardmaster

import (
	"bytes"
	"encoding/gob"
	"hash/crc32"
	"sync"

	"github.com/singlemonad/mit6.824/labrpc"
	"github.com/singlemonad/mit6.824/raft"
	"go.uber.org/zap"
)

const (
	Replicas = 999
)

func HashInt(key int) int {
	buff := new(bytes.Buffer)
	encoder := gob.NewEncoder(buff)
	if err := encoder.Encode(key); err != nil {
		panic(err)
	}
	return int(crc32.ChecksumIEEE(buff.Bytes()))
}

type OperationType = int

const (
	Operation_Join  = 1
	Operation_Leave = 2
	Operation_Move  = 3
	Operation_Query = 4
)

type OpRequest struct {
	uuid     string
	typ      OperationType
	args     interface{}
	reply    interface{}
	resultCh chan interface{}
}

func MakeOpRequest(uuid string, typ OperationType, args, reply interface{}) *OpRequest {
	return &OpRequest{
		uuid:     uuid,
		typ:      typ,
		args:     args,
		reply:    reply,
		resultCh: make(chan interface{}),
	}
}

type Op struct {
	Uuid  string
	Typ   OperationType
	Args  interface{}
	Reply interface{}
}

func MaterOp(encoder *gob.Encoder, command interface{}) error {
	op := command.(Op)
	if err := encoder.Encode(op); err != nil {
		return err
	}
	return nil
}

func DeMaterOp(decoder *gob.Decoder) (interface{}, error) {
	var op Op
	if err := decoder.Decode(&op); err != nil {
		return nil, err
	}
	return op, nil
}

type ShardMaster struct {
	logger     *zap.SugaredLogger
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	configs    []Config // indexed by config num
	hash       *Hash
	inReq      map[string]*OpRequest
	historyReq map[string]bool
	loopCh     chan *OpRequest
	applyCh    chan raft.ApplyMsg
	doneWg     sync.WaitGroup
	exitCh     chan interface{}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	logger, _ := zap.NewProduction()
	sm.logger = logger.Sugar()

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.hash = NewHash(Replicas, HashInt)

	sm.inReq = make(map[string]*OpRequest)
	sm.historyReq = make(map[string]bool)

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveArgs{})
	gob.Register(LeaveReply{})
	gob.Register(MoveArgs{})
	gob.Register(MoveReply{})

	sm.applyCh = make(chan raft.ApplyMsg, 10000)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh, MaterOp, DeMaterOp)

	sm.loopCh = make(chan *OpRequest, 10000)
	sm.exitCh = make(chan interface{})
	sm.doneWg = sync.WaitGroup{}

	sm.doneWg.Add(1)
	go sm.mainThread()

	return sm
}

// 在一组服务其上新起raft group
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	req := MakeOpRequest(args.Uuid, Operation_Join, args, reply)
	sm.loopCh <- req
	<-req.resultCh
}

// 停用raft group
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	req := MakeOpRequest(args.Uuid, Operation_Leave, args, reply)
	sm.loopCh <- req
	<-req.resultCh
}

// 将一个分片加入到一个raft group
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	req := MakeOpRequest(args.Uuid, Operation_Move, args, reply)
	sm.loopCh <- req
	<-req.resultCh
}

// 查询指定版本的配置
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	req := MakeOpRequest(args.Uuid, Operation_Query, args, reply)
	sm.loopCh <- req
	<-req.resultCh
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	close(sm.exitCh)
	sm.doneWg.Wait()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) mainThread() {
	defer func() {
		sm.doneWg.Done()
		sm.logger.Infow("shardmaster exit main loop", "me", sm.me)
	}()

	for {
		select {
		case req := <-sm.loopCh:
			switch req.typ {
			case Operation_Join:
				sm.join(req)
			case Operation_Leave:
				sm.leave(req)
			case Operation_Move:
				sm.move(req)
			case Operation_Query:
				sm.query(req)
			}
		case entry := <-sm.applyCh:
			sm.apply(entry)
		case <-sm.exitCh:
			return
		}
	}
}

func (sm *ShardMaster) join(req *OpRequest) {
	args := req.args.(*JoinArgs)
	reply := req.reply.(*JoinReply)

	if !sm.isLeader() {
		reply.WrongLeader = true
		req.resultCh <- struct{}{}
		return
	}

	if _, ok := sm.historyReq[req.uuid]; ok {
		req.resultCh <- struct{}{}
		return
	} else {
		sm.historyReq[req.uuid] = true
	}

	sm.inReq[req.uuid] = req
	_, _, isLeader := sm.rf.Start(Op{
		Uuid:  args.Uuid,
		Typ:   Operation_Join,
		Args:  *args,
		Reply: *reply,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) leave(req *OpRequest) {
	args := req.args.(*LeaveArgs)
	reply := req.reply.(*LeaveReply)

	if !sm.isLeader() {
		reply.WrongLeader = true
		req.resultCh <- struct{}{}
		return
	}

	if _, ok := sm.historyReq[req.uuid]; ok {
		req.resultCh <- struct{}{}
		return
	} else {
		sm.historyReq[req.uuid] = true
	}

	sm.inReq[req.uuid] = req
	_, _, isLeader := sm.rf.Start(Op{
		Uuid:  args.Uuid,
		Typ:   Operation_Leave,
		Args:  *args,
		Reply: *reply,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) move(req *OpRequest) {
	args := req.args.(*MoveArgs)
	reply := req.reply.(*MoveReply)

	if !sm.isLeader() {
		reply.WrongLeader = true
		req.resultCh <- struct{}{}
		return
	}

	if _, ok := sm.historyReq[req.uuid]; ok {
		req.resultCh <- struct{}{}
		return
	} else {
		sm.historyReq[req.uuid] = true
	}

	sm.inReq[req.uuid] = req
	_, _, isLeader := sm.rf.Start(Op{
		Uuid:  args.Uuid,
		Typ:   Operation_Move,
		Args:  *args,
		Reply: *reply,
	})
	if !isLeader {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) query(req *OpRequest) {
	args := req.args.(*QueryArgs)
	reply := req.reply.(*QueryReply)

	if !sm.isLeader() {
		reply.WrongLeader = true
		req.resultCh <- struct{}{}
		return
	}

	if args.Num < 0 {
		reply.Config = sm.latestConfig()
	} else if len(sm.configs) < args.Num {
		reply.Err = "Not exist"
	} else {
		reply.Config = sm.configs[args.Num]
	}
	req.resultCh <- struct{}{}
}

func (sm *ShardMaster) apply(entry raft.ApplyMsg) {
	if entry.UseSnapshot {
		sm.deMaterlize(entry.Snapshot)
		return
	}

	command := entry.Command.(Op)
	switch command.Typ {
	case Operation_Join:
		sm.doJoin(command.Args.(JoinArgs))
	case Operation_Leave:
		sm.doLeave(command.Args.(LeaveArgs))
	case Operation_Move:
		sm.doMove(command.Args.(MoveArgs))
	}

	if _, isLeader := sm.rf.GetState(); isLeader {
		//sm.logger.Infow("propose finished", "me", sm.me, "uuid", command.Uuid, "typ", command.Typ, "args", command.Args, "config", sm.configs)

		sm.logger.Infow("apply log", "me", sm.me, "uuid", command.Uuid, "typ", command.Typ, "args", command.Args, "config", sm.configs)

		if req, ok := sm.inReq[command.Uuid]; ok {
			delete(sm.inReq, command.Uuid)
			req.resultCh <- struct{}{}
		}
	} else if len(sm.inReq) != 0 {
		//sm.logger.Infow("Follower not support PutAppend", "me", sm.me)
	}
}

func (sm *ShardMaster) doJoin(args JoinArgs) {
	newCfg := sm.latestConfig().Clone()
	newCfg.Num++
	addGids := make([]int, 0)
	for gid, servers := range args.Servers {
		newCfg.Groups[gid] = servers
		addGids = append(addGids, gid)
	}
	sm.hash.Add(addGids)
	sm.reBalance(&newCfg)
	sm.configs = append(sm.configs, newCfg)
}

func (sm *ShardMaster) doLeave(args LeaveArgs) {
	newCfg := sm.latestConfig().Clone()
	newCfg.Num++
	delGids := make([]int, 0)
	for _, gid := range args.GIDs {
		delete(newCfg.Groups, gid)
		delGids = append(delGids, gid)
	}
	sm.hash.Delete(delGids)
	sm.reBalance(&newCfg)
	sm.configs = append(sm.configs, newCfg)
}

func (sm *ShardMaster) doMove(args MoveArgs) {
	newCfg := sm.latestConfig().Clone()
	newCfg.Num++
	if _, ok := newCfg.Groups[args.GID]; ok {
		newCfg.Shards[args.Shard] = args.GID
	}
	sm.configs = append(sm.configs, newCfg)
	sm.reBalance(&newCfg)
}

func (sm *ShardMaster) reBalance(cfg *Config) {
	for shardId, _ := range cfg.Shards {
		cfg.Shards[shardId] = sm.hash.Hash(shardId)
	}
}

func (sm *ShardMaster) latestConfig() Config {
	if len(sm.configs) == 0 {
		return Config{}
	}
	return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) isLeader() bool {
	_, isLeader := sm.rf.GetState()
	return isLeader
}

func (sm *ShardMaster) materlize() []byte {
	writer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writer)

	if err := encoder.Encode(len(sm.configs)); err != nil {
		panic(err)
	}
	for _, cfg := range sm.configs {
		if err := encoder.Encode(cfg); err != nil {
			panic(err)
		}
	}
	return writer.Bytes()
}

func (sm *ShardMaster) deMaterlize([]byte) {
	reader := new(bytes.Buffer)
	decoder := gob.NewDecoder(reader)

	var length int
	if err := decoder.Decode(&length); err != nil {
		panic(err)
	}
	sm.configs = make([]Config, 0)
	for i := 0; i < length; i++ {
		var cfg Config
		if err := decoder.Decode(&cfg); err != nil {
			panic(err)
		}
		sm.configs = append(sm.configs, cfg)
	}
	return
}
