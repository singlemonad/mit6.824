package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
// key最多分为10个分区
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
// 集群配置，包括以下两个方面
//  1. 每个raft group包含的分区
//  2. 每个raft group中raft节点所在的机器信息
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid，分区对应的gid，每个分区至多对应一个gid，一个gid可以包含多个分区
	Groups map[int][]string // gid -> servers[]，每个gid对应的raft group所在的服务器列表
}

func (cfg Config) Clone() Config {
	c := Config{
		Num:    cfg.Num,
		Shards: cfg.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range cfg.Groups {
		c.Groups[k] = make([]string, 0)
		for _, entry := range v {
			c.Groups[k] = append(c.Groups[k], entry)
		}
	}
	return c
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Uuid    string
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Uuid        string
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	Uuid string
	GIDs []int
}

type LeaveReply struct {
	Uuid        string
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Uuid  string
	Shard int
	GID   int
}

type MoveReply struct {
	Uuid        string
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Uuid string
	Num  int // desired config number
}

type QueryReply struct {
	Uuid        string
	WrongLeader bool
	Err         Err
	Config      Config
}
