package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/singlemonad/mit6.824/labrpc"
	"go.uber.org/zap"
)

// import "bytes"
// import "encoding/gob"

type Role = int

const (
	Leader    = 0
	Follower  = 1
	Candidate = 2
)

type stepFunc = func()

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logger    *zap.SugaredLogger
	role      Role
	term      int
	voteFor   int
	leader    int
	commit    int
	applied   int
	stepFunc  stepFunc
	raftLog   RaftLog
	nodeTotal int
	applyC    chan ApplyMsg
	loopC     chan *RequestHandle
	exitC     chan interface{}
	wg        sync.WaitGroup

	electionTimeout        int
	electionTimeoutCounter int
	voteHadGot             int

	nextIndexs  []int
	matchIndexs []int

	trustFollower map[int]bool // 日志和Leader保持一致的Follower
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	logger, _ := zap.NewProduction()
	rf := &Raft{
		mu:                     sync.Mutex{},
		peers:                  peers,
		persister:              persister,
		me:                     me,
		logger:                 logger.Sugar(),
		role:                   Follower,
		term:                   0,
		voteFor:                -1,
		leader:                 -1,
		commit:                 -1,
		applied:                -1,
		nodeTotal:              len(peers),
		applyC:                 applyCh,
		loopC:                  make(chan *RequestHandle, 10000),
		exitC:                  make(chan interface{}),
		wg:                     sync.WaitGroup{},
		electionTimeout:        10 + int(rand.NewSource(time.Now().UnixNano()).Int63()%int64(20)),
		electionTimeoutCounter: 0,
	}
	rf.stepFunc = rf.electionTimeoutMonitor

	/// initialize from state persisted before a crash
	//	r initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())

	//rf.logger.Infow("raft node state", "me", rf.me, "term", rf.term, "vote", rf.voteFor, "applied", rf.applied, "logs", rf.raftLog.GetLogs(0))

	rf.wg.Add(1)
	go rf.mainLoop()

	return rf
}

type RequestType = int

const (
	RequestVote   = 1
	AppendEntries = 2
	GetStates     = 3
	Propose       = 4
)

type RequestHandle struct {
	typ      RequestType
	args     interface{}
	reply    interface{}
	resultCh chan interface{}
}

func makeRequestHandle(typ RequestType, args, reply interface{}) *RequestHandle {
	return &RequestHandle{
		typ:      typ,
		args:     args,
		reply:    reply,
		resultCh: make(chan interface{}),
	}
}

type RequestVoteArgs struct {
	Id        int
	Term      int
	LastIndex int
	LastTerm  int
}

type RequestVoteReply struct {
	Term   int
	Reject bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	h := makeRequestHandle(RequestVote, args, reply)
	rf.loopC <- h
	<-h.resultCh
	return
}

type AppendEntriesArgs struct {
	Id          int
	Term        int
	Commit      int
	PreLogIndex int
	PreLogTerm  int
	Entries     []*Entry
}

type AppendEntriesReply struct {
	Term   int
	Reject bool

	// Reject为true时通知Leader节点日志匹配的term与index
	MatchIndex int
	MatchTerm  int

	// 心跳时通知Leader节点日志最新term与index
	LatestIndex int
	LatestTerm  int
}

func (rf *Raft) RequestAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	h := makeRequestHandle(AppendEntries, args, reply)
	rf.loopC <- h
	<-h.resultCh
	return
}

// return currentTerm and whether this server
// believes it is the leader.
type GetStateReply struct {
	term     int
	isLeader bool
}

func (rf *Raft) GetState() (int, bool) {
	reply := &GetStateReply{}
	h := makeRequestHandle(GetStates, nil, reply)
	rf.loopC <- h
	<-h.resultCh
	return reply.term, reply.isLeader
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

type ProposeArgs struct {
	command interface{}
}

type ProposeReply struct {
	term     int
	index    int
	isLeader bool
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	reply := &ProposeReply{}
	h := makeRequestHandle(Propose, ProposeArgs{command}, reply)
	rf.loopC <- h
	<-h.resultCh
	return reply.index, reply.term, rf.isLeader()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	close(rf.exitC)
	rf.wg.Wait()
}

// iterate loopC to handle request
func (rf *Raft) mainLoop() {
	defer func() {
		rf.wg.Done()
		//rf.logger.Infow("main loop exit", "id", rf.me)
	}()

	//rf.logger.Infow("raft start", "md", rf.me)

	stepTicker := time.NewTicker(time.Millisecond * 100)
	applyTicker := time.NewTicker(time.Millisecond * 10)
	syncFollowerTicker := time.NewTicker(time.Millisecond * 200)
	for {
		select {
		case <-stepTicker.C:
			rf.stepFunc()
		case <-applyTicker.C:
			rf.applyCommand()
		case <-syncFollowerTicker.C:
			if rf.isLeader() {
				rf.syncFollower()
			}
			//rf.logger.Infow("I'm Live", "me", rf.me, "role", rf.role)
		case req := <-rf.loopC:
			switch req.typ {
			case RequestVote:
				rf.requestVote(req)
			case AppendEntries:
				rf.appendEntries(req)
			case GetStates:
				rf.getState(req)
			case Propose:
				rf.propose(req)
			}
		case <-rf.exitC:
			return
		}
	}
}

func (rf *Raft) requestVote(req *RequestHandle) {
	args := req.args.(RequestVoteArgs)
	reply := req.reply.(*RequestVoteReply)

	//rf.logger.Infow("receive RequestVote", "me", rf.me, "term", rf.term, "args", args)

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Reject = true
		req.resultCh <- struct{}{}
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
	}
	canVote := rf.voteFor == args.Id ||
		(rf.voteFor == -1 && rf.raftLog.IsUptoDate(args.LastIndex, args.LastTerm))
	if canVote {
		rf.voteFor = args.Id
		rf.electionTimeoutCounter = 0
		rf.becomeFollower(args.Term, -1)
	} else {
		rf.logger.Infow("reject vote", "me", rf.me, "term", rf.term, "vote", rf.voteFor, "args", args, "lastIndex", rf.raftLog.LastIndex(), "lastTerm", rf.raftLog.LastTerm())

		reply.Reject = true
	}
	reply.Term = rf.term
	req.resultCh <- struct{}{}

}

func (rf *Raft) appendEntries(req *RequestHandle) {
	args := req.args.(AppendEntriesArgs)
	reply := req.reply.(*AppendEntriesReply)

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Reject = true
		req.resultCh <- struct{}{}
		return
	}

	if len(args.Entries) == 0 {
		if rf.leader != args.Id {
			rf.leader = args.Id
		}

		if args.Commit > rf.commit {
			applyIndex := min(args.Commit, rf.raftLog.LastIndex())
			if rf.commit < applyIndex {
				rf.commit = applyIndex
			}
		}

		rf.electionTimeoutCounter = 0
		rf.becomeFollower(args.Term, args.Id)
		rf.persist()

		rf.term = args.Term
		reply.Term = rf.term
		reply.LatestTerm = rf.raftLog.LastTerm()
		reply.LatestIndex = rf.raftLog.LastIndex()
		req.resultCh <- struct{}{}
		return
	}

	rf.logger.Infow("receive AppendEntries", "me", rf.me, "term", rf.term, "args", args)

	if rf.raftLog.Match(args.PreLogIndex, args.PreLogTerm) {
		rf.raftLog.Catoff(args.PreLogIndex + 1)
		rf.raftLog.Append(args.Entries)
		rf.persist()
		applyIndex := min(args.Commit, rf.raftLog.LastIndex())
		if rf.commit < applyIndex {
			rf.commit = applyIndex
		}
	} else {
		rf.logger.Infow("AppendEntries not match", "me", rf.me, "term", rf.term, "my term", rf.raftLog.GetLogTerm(args.PreLogIndex), "leader term", args.PreLogTerm)

		reply.MatchIndex = rf.raftLog.GetMaxMatchIndex(args.PreLogTerm)
		if reply.MatchIndex == -1 {
			reply.MatchTerm = rf.raftLog.LastTerm()
		} else {
			reply.MatchTerm = args.Term
		}
		reply.Reject = true
	}
	rf.term = args.Term
	reply.Term = rf.term
	req.resultCh <- struct{}{}

	return
}

func (rf *Raft) getState(req *RequestHandle) {
	reply := req.reply.(*GetStateReply)
	reply.term = rf.term
	reply.isLeader = rf.isLeader()
	req.resultCh <- struct{}{}
}

func (rf *Raft) propose(req *RequestHandle) {
	args := req.args.(ProposeArgs)
	reply := req.reply.(*ProposeReply)

	if !rf.isLeader() {
		reply.term = -1
		reply.index = -1
		reply.isLeader = false
		req.resultCh <- struct{}{}
		return
	}

	//rf.logger.Infow("propose", "me", rf.me, "term", rf.term, "data", args.command)

	rf.raftLog.Append([]*Entry{&Entry{rf.term, args.command}})
	rf.nextIndexs[rf.me] = rf.raftLog.LastIndex() + 1
	rf.matchIndexs[rf.me] = rf.raftLog.LastIndex()
	rf.persist()

	for id, _ := range rf.peers {
		if id == rf.me {
			continue
		}

		appendReply := &AppendEntriesReply{}
		succeed := rf.sendAppendEntries(id, AppendEntriesArgs{
			Id:          rf.me,
			Term:        rf.term,
			Commit:      rf.commit,
			PreLogIndex: rf.nextIndexs[id] - 1,
			PreLogTerm:  rf.raftLog.GetLogTerm(rf.nextIndexs[id] - 1),
			Entries:     rf.raftLog.GetLogs(rf.nextIndexs[id]),
		}, appendReply)

		if succeed {
			if !rf.appendEntriesReply(appendReply, id) {
				reply.term = -1
				reply.index = -1
				reply.isLeader = false
				req.resultCh <- struct{}{}
				return
			}
		}
	}

	reply.term = rf.term
	reply.index = rf.raftLog.LastIndex()
	reply.isLeader = true
	req.resultCh <- struct{}{}

	return
}

func (rf *Raft) appendEntriesReply(reply *AppendEntriesReply, id int) bool {
	if reply.Term > rf.term {
		rf.becomeFollower(reply.Term, -1)
		return false
	}

	// heartbeat reply
	if reply.LatestTerm != 0 && reply.LatestIndex != 0 {
		if rf.raftLog.GetLogTerm(reply.LatestIndex) == reply.LatestTerm {
			rf.nextIndexs[id] = reply.LatestIndex + 1
			rf.trustFollower[id] = true
		} else {
			rf.nextIndexs[id] = reply.LatestIndex
		}
		return true
	}

	// actual append log reply
	if reply.Reject {
		if reply.MatchIndex == -1 {
			rf.nextIndexs[id] = rf.raftLog.GetMaxMatchIndex(reply.MatchTerm) + 1
		} else {
			rf.nextIndexs[id] = reply.MatchIndex + 1
		}
	} else {
		rf.nextIndexs[id] = rf.raftLog.LastIndex() + 1
		rf.matchIndexs[id] = rf.raftLog.LastIndex()
		rf.updateCommit()
		rf.trustFollower[id] = true
	}
	return true
}

func (rf *Raft) updateCommit() {
	matchArr := make([]int, len(rf.matchIndexs))
	copy(matchArr, rf.matchIndexs)
	sort.Slice(matchArr, func(i, j int) bool {
		return matchArr[i] < matchArr[j]
	})
	if matchArr[len(matchArr)/2] > rf.commit {
		rf.commit = matchArr[len(matchArr)/2]
	}
}

func (rf *Raft) applyCommand() {
	for index := rf.applied + 1; index <= rf.commit; index++ {
		msg := ApplyMsg{
			Index:       index,
			Command:     rf.raftLog.GetLog(index).Data,
			UseSnapshot: false,
			Snapshot:    nil,
		}
		rf.applyC <- msg
		rf.applied++
		rf.persist()

		rf.logger.Infow("apply log", "me", rf.me, "term", rf.term, "log", msg)
	}
}

func (rf *Raft) syncFollower() {
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			continue
		}

		if rf.nextIndexs[id] <= rf.raftLog.LastIndex() {
			reply := &AppendEntriesReply{}
			succeed := rf.sendAppendEntries(id, AppendEntriesArgs{
				Id:          rf.me,
				Term:        rf.term,
				Commit:      rf.commit,
				PreLogIndex: rf.nextIndexs[id] - 1,
				PreLogTerm:  rf.raftLog.GetLogTerm(rf.nextIndexs[id] - 1),
				Entries:     rf.raftLog.GetLogs(rf.nextIndexs[id]),
			}, reply)

			if succeed {
				rf.appendEntriesReply(reply, id)
			}
		}
	}
}

func (rf *Raft) isLeader() bool {
	return rf.me == rf.leader
}

func (rf *Raft) electionTimeoutMonitor() {
	rf.electionTimeoutCounter++
	if rf.electionTimeoutCounter >= rf.electionTimeout {
		rf.electionTimeoutCounter = 0
		rf.becomeCandidate()
		rf.startElection()
	}
}

func (rf *Raft) startElection() {
	rf.logger.Infow("start election", "me", rf.me, "term", rf.term)

	beLeader := false
	for id, _ := range rf.peers {
		if id == rf.me {
			continue
		}

		reply := &RequestVoteReply{}
		succeed := rf.sendRequestVote(id, RequestVoteArgs{
			Id:        rf.me,
			Term:      rf.term,
			LastIndex: rf.raftLog.LastIndex(),
			LastTerm:  rf.raftLog.LastTerm(),
		}, reply)

		if succeed {
			//rf.logger.Infow("recv RequestVote reply", "me", rf.me, "from", id, "reply", reply)

			if reply.Term < rf.term {
				continue
			}

			if reply.Term > rf.term {
				rf.electionTimeoutCounter = 0
				rf.becomeFollower(reply.Term, -1)
				return
			}

			if reply.Reject == false {
				rf.voteHadGot++
				if rf.voteHadGot >= rf.nodeTotal/2+1 {
					rf.electionTimeoutCounter = 0
					rf.becomeLeader()
					beLeader = true
					return
				}
			}
		}
	}
	if !beLeader {
		rf.electionTimeoutCounter = 0
		rf.electionTimeout = 10 + int(rand.NewSource(time.Now().UnixNano()).Int63()%int64(20))
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.leader = rf.me
	rf.stepFunc = rf.leaderHeartbeat
	rf.trustFollower = make(map[int]bool)

	rf.nextIndexs = make([]int, rf.nodeTotal)
	rf.matchIndexs = make([]int, rf.nodeTotal)
	for id := 0; id < rf.nodeTotal; id++ {
		rf.nextIndexs[id] = rf.raftLog.LastIndex() + 1
		if id == rf.me {
			rf.matchIndexs[id] = rf.raftLog.LastIndex()
		} else {
			rf.matchIndexs[id] = -1
		}
	}
	rf.persist()

	rf.logger.Infow("become leader", "id", rf.me, "term", rf.term, "role", rf.role)

	rf.leaderHeartbeat()
}

func (rf *Raft) becomeCandidate() {
	rf.term++
	rf.voteHadGot = 0
	rf.voteFor = rf.me
	rf.voteHadGot++
}

func (rf *Raft) becomeFollower(term, leader int) {
	if rf.role != Follower {
		rf.logger.Infow("become follower", "me", rf.me, "term", rf.term, "pre role", rf.role)
	}

	rf.role = Follower
	rf.term = term
	rf.leader = leader
	rf.stepFunc = rf.electionTimeoutMonitor
}

func (rf *Raft) leaderHeartbeat() {
	for id, _ := range rf.peers {
		if id == rf.me {
			continue
		}
		reply := &AppendEntriesReply{}
		args := AppendEntriesArgs{
			Id:      rf.me,
			Term:    rf.term,
			Entries: nil,
		}
		if _, ok := rf.trustFollower[id]; ok {
			args.Commit = rf.commit
		}
		rf.sendAppendEntries(id, args, reply)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	//rf.logger.Infow("send RequestVote request", "me", rf.me, "term", rf.term, "to", server, "args", args)

	finishC := make(chan bool)
	go func(chan bool) {
		finishC <- rf.peers[server].Call("Raft.RequestVote", args, reply)
	}(finishC)

	select {
	case ok := <-finishC:
		return ok
	case <-time.After(time.Millisecond * 30):
		//rf.logger.Errorw("send RequestVote tiemout", "me", rf.me, "term", rf.term, "to", server)
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.logger.Infow("send AppendEntries request", "me", rf.me, "term", rf.term, "to", server, "args", args)

	finishC := make(chan bool)
	go func(chan bool) {
		finishC <- rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	}(finishC)

	select {
	case ok := <-finishC:
		return ok
	case <-time.After(time.Millisecond * 30):
		//rf.logger.Errorw("send AppendEntries tiemout", "me", rf.me, "term", rf.term, "to", server, "args", args, "reply", reply)
		return false
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// save raft state
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(rf.term); err != nil {
		panic(err.Error())
	}
	if err := e.Encode(rf.voteFor); err != nil {
		panic(err.Error())
	}
	if err := e.Encode(rf.applied); err != nil {
		panic(err.Error())
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	// save snapshot
	rf.persister.SaveSnapshot(rf.raftLog.Persist())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// restore raft state
	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		if err := d.Decode(&rf.term); err != nil {
			panic(err.Error())
		}
		if err := d.Decode(&rf.voteFor); err != nil {
			panic(err.Error())
		}
		if err := d.Decode(&rf.applied); err != nil {
			panic(err.Error())
		}
	}

	// restore snapshot
	rf.raftLog = NewMemoryLog(rf.persister.ReadSnapshot())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
