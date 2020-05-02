package raftkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"time"

	"go.uber.org/zap"

	uuid "github.com/satori/go.uuid"
	"github.com/singlemonad/mit6.824/labrpc"
	"github.com/singlemonad/mit6.824/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OperationType = int

const (
	OperationType_Get    = 1
	OperationType_Put    = 2
	OperationType_Append = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Uuid  string
	Typ   OperationType
	Key   string
	Value string
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
	err := decoder.Decode(&op)
	return op, err
}

type OpRequest struct {
	Uuid     string
	Typ      OperationType
	Args     interface{}
	Reply    interface{}
	ResultCh chan interface{}
}

func MakeOpReqeust(uuid string, typ OperationType, args interface{}, reply interface{}) *OpRequest {
	return &OpRequest{
		Uuid:     uuid,
		Typ:      typ,
		Args:     args,
		Reply:    reply,
		ResultCh: make(chan interface{}),
	}
}

type RaftKV struct {
	logger     *zap.SugaredLogger
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	loopCh     chan *OpRequest
	kvs        map[string]string
	inReq      map[string]*OpRequest
	historyReq map[string]bool
	exitCh     chan interface{}
	wg         sync.WaitGroup

	maxraftstate int // snapshot if log grows this big
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	logger, _ := zap.NewProduction()
	kv.logger = logger.Sugar()

	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, MaterOp, DeMaterOp)
	kv.loopCh = make(chan *OpRequest, 10000)
	kv.kvs = make(map[string]string)
	kv.inReq = make(map[string]*OpRequest)
	kv.historyReq = make(map[string]bool)
	kv.exitCh = make(chan interface{})
	kv.wg = sync.WaitGroup{}

	kv.wg.Add(1)
	go kv.mainLoop()

	return kv
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	uuid := uuid.NewV4().String()
	req := MakeOpReqeust(uuid, OperationType_Get, args, reply)
	kv.loopCh <- req
	<-req.ResultCh
	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//kv.logger.Infow("putAppend request", "args", args)
	var typ OperationType
	if args.Op == "Put" {
		typ = OperationType_Put
	} else {
		typ = OperationType_Append
	}

	req := MakeOpReqeust(args.Uuid, typ, args, reply)
	kv.loopCh <- req
	select {
	case <-req.ResultCh:
		return
	case <-time.After(time.Second * 3):
		reply.Err = "ErrTimeout"

		kv.logger.Errorw("PutAppend timeout", "args", args, "reply", reply)
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	close(kv.exitCh)
	kv.wg.Wait()
}

func (kv *RaftKV) mainLoop() {
	defer func() {
		kv.wg.Done()
		kv.logger.Infow("main loop exit", "me", kv.me)
	}()

	//ticker := time.NewTicker(time.Millisecond * 500)
	for {
		select {
		//case <-ticker.C:
		//kv.logger.Infow("I'm live", "me", kv.me)
		case req := <-kv.loopCh:
			switch req.Typ {
			case OperationType_Get:
				kv.get(req)
			case OperationType_Put, OperationType_Append:
				kv.putAppend(req)
			}
		case entry := <-kv.applyCh:
			kv.apply(entry)
		case <-kv.exitCh:
			return
		}
	}
}

func (kv *RaftKV) get(req *OpRequest) {
	args := req.Args.(*GetArgs)
	reply := req.Reply.(*GetReply)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		req.ResultCh <- struct{}{}
		return
	}

	if val, ok := kv.kvs[args.Key]; ok {
		reply.Value = val
		reply.Err = OK
		req.ResultCh <- struct{}{}
		return
	}
	reply.Err = ErrNoKey
	req.ResultCh <- struct{}{}
}

func (kv *RaftKV) putAppend(req *OpRequest) {
	args := req.Args.(*PutAppendArgs)
	reply := req.Reply.(*PutAppendReply)

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		req.ResultCh <- struct{}{}
		return
	}

	if _, ok := kv.historyReq[args.Uuid]; ok {
		kv.logger.Infow("duplicate putAppend request", "me", kv.me, "uuid", args.Uuid)

		req.ResultCh <- struct{}{}
		return
	} else {
		kv.historyReq[args.Uuid] = true
	}

	//kv.logger.Infow("putAppend request", "me", kv.me, "uuid", req.Uuid, "args", args)

	var op OperationType
	if args.Op == "Put" {
		op = OperationType_Put
	} else {
		op = OperationType_Append
	}
	_, _, isLeader := kv.rf.Start(Op{
		Uuid:  req.Uuid,
		Typ:   op,
		Key:   args.Key,
		Value: args.Value,
	})
	if !isLeader {
		reply.WrongLeader = true
		req.ResultCh <- struct{}{}
		return
	}
	kv.inReq[req.Uuid] = req
}

func (kv *RaftKV) apply(msg raft.ApplyMsg) {
	if msg.UseSnapshot {
		kv.kvs = kv.deMaterlize(msg.Snapshot)
		return
	}

	command := msg.Command.(Op)
	switch command.Typ {
	case OperationType_Put:
		kv.kvs[command.Key] = command.Value
	case OperationType_Append:
		newVal := kv.kvs[command.Key] + command.Value
		kv.kvs[command.Key] = newVal
	}
	//kv.logger.Infow("kv", "key", command.Key, "value", kv.kvs[command.Key])
	if _, isLeader := kv.rf.GetState(); isLeader {
		//kv.logger.Infow("putAppend finished", "me", kv.me, "uuid", command.Uuid, "typ", command.Typ, "key", command.Key, "value", command.Value)

		if req, ok := kv.inReq[command.Uuid]; ok {
			delete(kv.inReq, command.Uuid)
			req.ResultCh <- struct{}{}
		}
	} else if len(kv.inReq) != 0 {
		kv.logger.Infow("Follower not support PutAppend", "me", kv.me)
	}
}

func (kv *RaftKV) doSnapshot() {
	if kv.maxraftstate == -1 {
		return
	}

	if kv.rf.GetRaftSate() > kv.maxraftstate {
		kv.logger.Infow("do snapshot", "maxraftstate", kv.maxraftstate, "raftstate", kv.rf.GetRaftSate())

		kv.rf.RaftLogOversize(kv.materlize())
	}
}

func (kv *RaftKV) materlize() []byte {
	writer := new(bytes.Buffer)
	encoder := gob.NewEncoder(writer)

	if err := encoder.Encode(len(kv.kvs)); err != nil {
		panic(err)
	}
	for k, v := range kv.kvs {
		if err := encoder.Encode(k); err != nil {
			panic(err)
		}
		if err := encoder.Encode(v); err != nil {
			panic(err)
		}
	}
	return writer.Bytes()
}

func (kv *RaftKV) deMaterlize([]byte) map[string]string {
	reader := new(bytes.Buffer)
	decoder := gob.NewDecoder(reader)

	var length int
	if err := decoder.Decode(&length); err != nil {
		panic(err)
	}
	kvs := make(map[string]string)
	for i := 0; i < length; i++ {
		var k, v string
		if err := decoder.Decode(&k); err != nil {
			panic(err)
		}
		if err := decoder.Decode(&v); err != nil {
			panic(err)
		}
		kvs[k] = v
	}
	return kvs
}
