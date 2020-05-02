package raftkv

import (
	"crypto/rand"
	"math/big"
	"time"

	uuid2 "github.com/satori/go.uuid"

	"go.uber.org/zap"

	"github.com/singlemonad/mit6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	logger  *zap.SugaredLogger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	logger, _ := zap.NewProduction()
	ck.logger = logger.Sugar()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	for i := 0; i < 1000; i++ {
		for id, _ := range ck.servers {
			args := &GetArgs{key}
			reply := &GetReply{}
			if ok := ck.servers[id].Call("RaftKV.Get", args, reply); ok {
				if !reply.WrongLeader {
					if reply.Err == OK {
						return reply.Value
					} else {
						ck.logger.Errorw("Get failed", "key", key)
						return ""
					}
				}
			}
		}
		<-time.After(time.Millisecond * 10)
	}
	ck.logger.Errorw("no leader exist", "key", key)
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	uuid := uuid2.NewV4().String()
	for i := 0; i < 1000; i++ {
		for id, _ := range ck.servers {
			args := &PutAppendArgs{uuid, key, value, op}
			reply := &PutAppendReply{}
			if ok := ck.servers[id].Call("RaftKV.PutAppend", args, reply); ok {
				if !reply.WrongLeader {
					return
				}
			}
		}
		<-time.After(time.Millisecond * 10)
	}
	ck.logger.Errorw("no leader exist", "key", key, "value", value, "op", op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
