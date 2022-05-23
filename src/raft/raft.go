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

	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Leader
	Candidate
)

func (this State) String() string {
	switch this {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return "Unknow"
	}
}

// Millisecond
const heartbeatTimeLap = 150          //50
const electionTimeout = 1000          // 1000
const sleepLapTimeBeforeElection = 50 // 200
const appendEntriesTetryTimeLap = 50
const commitTimeLap = 20
const waitForVoteTimeout = 200 // 200
const waitForVoteTime = 20

// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	curState State

	currentTerm int
	voteFor     int
	voteSum     int
	rejectSum   int

	log Log

	commitIndex int
	lastApplied int

	nextIndex    []int
	matchedIndex []int

	commitCount map[int]int

	lastAppendTime time.Time

	applyCh chan ApplyMsg

	valided    []bool
	applyIndex int

	snapshot          []byte
	lastSnapshotIndex int
}

type Loginfo struct {
	Term    int
	Command interface{}
}

type Log struct {
	Logs   []Loginfo
	Offset int
}

func (log *Log) Len() int {
	return len(log.Logs)
}

func (log *Log) Get(index int) Loginfo {
	return log.Logs[index]
}

func (log *Log) Set(index int, l Loginfo) {
	log.Logs[index] = l
}

func (log *Log) Append(l Loginfo) {
	log.Logs = append(log.Logs, l)
}

func (log *Log) GetRange(start int, end int) []Loginfo {
	return log.Logs[start:end]
}

func (log *Log) SetLogs(other []Loginfo) {
	log.Logs = other
}

func (log *Log) Valid() bool {
	return log.Logs != nil || log.Len() > 0
}

// get Log last item's index and term
func (rf *Raft) GetLastItemInfo() (int, int) {
	l := rf.log.Len()
	if l == 0 {
		return -1, 0
	} else {
		return l - 1, rf.log.Get(l - 1).Term
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.curState == Leader)
	// Your code here (2A).
	return term, isleader
}

type InstallSnapshotArgs struct {
	Term              int
	LearderId         int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotRepl struct {
	Term int
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	raw_data := new(bytes.Buffer)
	encoder := labgob.NewEncoder(raw_data)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.voteFor)
	encoder.Encode(rf.log)
	data := raw_data.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	raw_data := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(raw_data)
	var term int
	var vote_for int
	var log Log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if decoder.Decode(&term) != nil ||
		decoder.Decode(&vote_for) != nil ||
		decoder.Decode(&log) != nil {
		DPrintf(dError, "read persist : decode data error", rf.me)
	} else {
		rf.currentTerm = term
		rf.voteFor = vote_for
		rf.log = log
	}

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) InstallSnapshot(arg *InstallSnapshotArgs, reply *InstallSnapshotRepl) {

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastSnapshotIndex {
		DPrintf(dError, "Snapshot index is smaller %v < %v", rf.me, index, rf.lastSnapshotIndex)
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      Log
	LeaderCommit int

	Valid bool
}

func (appendArgs *AppendEntriesArgs) String() string {
	t := "(Term:" + strconv.Itoa(appendArgs.Term)
	l := ",LeaderId:" + strconv.Itoa(appendArgs.LeaderId)
	p := ",PrevLogIndex:" + strconv.Itoa(appendArgs.PrevLogIndex)
	p1 := ",PrevLogTerm:" + strconv.Itoa(appendArgs.PrevLogTerm)
	e := ",Entries:" + fmt.Sprint(appendArgs.Entries)
	l1 := ",LeaderCommit:" + strconv.Itoa(appendArgs.LeaderCommit)
	return t + l + p + p1 + e + l1 + ")"
}

type AppendEntriesRepl struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, repl *AppendEntriesRepl) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.voteFor = -1
	repl.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		repl.Success = false
		return
	} else if args.Term >= rf.currentTerm && rf.curState != Follower {
		rf.curState = Follower
		for i := range rf.valided {
			rf.valided[i] = false
		}
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	} else {
		rf.lastAppendTime = time.Now()
	}
	if args.Entries.Valid() {
		if args.PrevLogIndex > rf.log.Len() ||
			(args.PrevLogIndex < rf.log.Len() && args.PrevLogTerm != rf.log.Get(args.PrevLogIndex).Term) {
			repl.Success = false
		} else if args.PrevLogIndex > 0 && args.PrevLogIndex == rf.log.Len() && rf.log.Get(rf.log.Len()-1).Term != args.PrevLogTerm {
			repl.Success = false
		} else {
			i := 0
			j := args.PrevLogIndex
			for ; j < rf.log.Len() && i < args.Entries.Len(); j, i = j+1, i+1 {
				if args.Entries.Get(i).Term != rf.log.Get(j).Term {
					break
				}
			}
			for ; i < args.Entries.Len(); j, i = j+1, i+1 {
				if j < rf.log.Len() {
					rf.log.Set(j, args.Entries.Get(i))
					DPrintf(dLog, "Save Log %v in T%v applyIndex:%v index:%v",
						rf.me, rf.log.Get(j), rf.currentTerm, rf.applyIndex, j)
				} else {
					rf.log.Append(args.Entries.Get(i))
					DPrintf(dLog, "Save Log %v in T%v applyIndex:%v index:%v",
						rf.me, args.Entries.Get(i), rf.currentTerm, rf.applyIndex, j)
				}
			}
			// in case : old log still in high index
			for ; j < rf.log.Len(); j = j + 1 {
				if rf.log.Get(j).Term < args.Term {
					rf.log.SetLogs(rf.log.GetRange(0, j))
					break
				}
			}
			repl.Success = true
			if rf.commitIndex < args.LeaderCommit {
				lastIndex := Max(rf.log.Len()-1, 0)
				rf.commitIndex = Min(lastIndex, args.LeaderCommit)
			}
		}
		DPrintf(dRetn, "<- S%d recv Append %s, log %v reply:%v",
			rf.me, args.LeaderId, args, rf.log, repl)
	} else {
		if args.Valid {
			for i := rf.applyIndex; i < rf.log.Len() && i <= args.LeaderCommit; i++ {
				amsg := ApplyMsg{}
				amsg.Command = rf.log.Get(i).Command
				amsg.CommandIndex = i + 1
				amsg.CommandValid = true
				rf.persist()
				rf.applyCh <- amsg
				rf.applyIndex += 1
				DPrintf(dCommit, "Commit %v(%v), applyIndex:%v, rf.log:%v",
					rf.me, rf.log.Get(i), i, rf.applyIndex, rf.log)
			}
		}
		repl.Success = true
		// DPrintf(dRetn, "<- S%d recv broadcast %s, log %v rf.applyIndex:%v",
		// 	rf.me, args.LeaderId, args, rf.log.Len(), rf.applyIndex)
	}
	// DPrintf(dRetn, "<- S%d recv Append %s, log %v reply:%v", rf.me, args.LeaderId, args, len(rf.log), repl)
}

func Min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var res string
	reply.Term = rf.currentTerm
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
		res = "vote for others"
	} else if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		res = "term to small"
	} else {
		lastIndex, lastTerm := rf.GetLastItemInfo()
		if lastTerm > args.LastLogTerm {
			reply.VoteGranted = false
			res = "log term to small"
		} else if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
			reply.VoteGranted = false
			res = "log index to small "
		} else {
			reply.VoteGranted = true
			rf.lastAppendTime = time.Now()
			rf.voteFor = args.CandidateId
			res = "granted"
		}
	}
	DPrintf(dVote, "recv Vote from S%v(T%v), cur vote: S%v(T%v), result: %v, Log:%v",
		rf.me, args.CandidateId, args.Term, rf.voteFor, rf.currentTerm, res, rf.log)
}

func (rf *Raft) electLeaderFromVoter(voter int) {
	args := RequestVoteArgs{}
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex, args.LastLogTerm = rf.GetLastItemInfo()
	rf.mu.Unlock()
	repl := RequestVoteReply{}
	DPrintf(dVote, "send vote rpc for S%v, log: %v", rf.me, voter, rf.log)
	ok := rf.sendRequestVote(voter, &args, &repl)
	if ok && repl.VoteGranted {
		rf.mu.Lock()
		rf.voteSum += 1
		DPrintf(dVote, "voted by S%v %v/%v", rf.me, voter, rf.voteSum, len(rf.peers))
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		DPrintf(dVote, "not voted by S%v %v/%v", rf.me, voter, rf.voteSum, len(rf.peers))
		if repl.Term > rf.currentTerm {
			rf.currentTerm = repl.Term
			rf.curState = Follower
		}
		rf.mu.Unlock()
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppnendEntries(server int, args *AppendEntriesArgs, repl *AppendEntriesRepl) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, repl)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	index, term := rf.GetLastItemInfo()
	index = index + 1
	if !(rf.curState == Leader) {
		rf.mu.Unlock()
		return index + 1, term, false
	}
	DPrintf(dInfo, "start command %v", rf.me, command)
	rf.log.Append(Loginfo{Term: rf.currentTerm, Command: command})
	DPrintf(dLog, "Save Log %v in T%v Remain:%v  index:%v",
		rf.me, command, rf.currentTerm, rf.applyIndex, index)
	rf.mu.Unlock()
	go rf.LogReplicate(index)
	return index + 1, term, true
}

func (rf *Raft) LogReplicate(index int) {
	rf.mu.Lock()
	rf.commitCount[index] = 1
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendAppnendEntriesUntilWin(i, false, index)
		}
	}
	flag := false
	checkCommitFunc := func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return rf.commitCount[index] > len(rf.peers)/2 || rf.commitIndex >= index
	}
	for rf.killed() == false {
		if checkCommitFunc() {
			flag = true
			break
		}
		time.Sleep(time.Duration(commitTimeLap * time.Millisecond))
	}
	if flag {
		rf.mu.Lock()
		rf.commitIndex = Max(index, rf.commitIndex)
		rf.mu.Unlock()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.checkElection() {
			// continue tick
			time.Sleep(time.Duration(sleepLapTimeBeforeElection * int(time.Millisecond)))
		} else {
			// wait a random time to start elections
			if rf.electLeader() {
				rf.mu.Lock()
				lastLogIndex := rf.log.Len()
				for i := range rf.peers {
					if i >= len(rf.nextIndex) {
						rf.nextIndex = append(rf.nextIndex, lastLogIndex)
					} else {
						rf.nextIndex[i] = lastLogIndex
					}
					if i >= len(rf.matchedIndex) {
						rf.matchedIndex = append(rf.matchedIndex, 0)
					} else {
						rf.matchedIndex[i] = 0
					}
					if i >= len(rf.valided) {
						rf.valided = append(rf.valided, false)
					} else {
						rf.valided[i] = false
					}
				}
				DPrintf(dInfo, "become leader %v/%v at T%v", rf.me, rf.voteSum, len(rf.peers), rf.currentTerm)
				rf.mu.Unlock()
				// broadcast immediate prevent others become leader
				go rf.heartbeat()
			} else {
				rf.mu.Lock()
				// rf.lastAppendTime = time.Now()
				DPrintf(dInfo, "vote failed %v/%v at T%v", rf.me, rf.voteSum, len(rf.peers), rf.currentTerm)
				rf.mu.Unlock()
			}
		}
	}
	DPrintf(dDrop, "(%v) ticker is over at T%v", rf.me, rf.curState, rf.curState)
}

func (rf *Raft) heartbeat() {
	checkLeader := func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return rf.curState == Leader
	}
	DPrintf(dDrop, "start heartbeat", rf.me)
	for rf.killed() == false && checkLeader() {
		rf.mu.Lock()
		for i := rf.applyIndex; i < rf.log.Len() && i <= rf.commitIndex; i++ {
			amsg := ApplyMsg{}
			amsg.Command = rf.log.Get(i).Command
			amsg.CommandIndex = i + 1
			amsg.CommandValid = true
			rf.persist()
			rf.applyCh <- amsg
			rf.applyIndex += 1
			DPrintf(dCommit, "Commit %v(%v), applyIndex:%v, rf.log:%v", rf.me, rf.log.Get(i), i, rf.applyIndex, rf.log)
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendAppnendEntriesUntilWin(i, true, 0)
			}
		}
		time.Sleep(time.Duration(heartbeatTimeLap * time.Millisecond))
	}
	DPrintf(dDrop, "(%v) heartbeat is over at T%v", rf.me, rf.curState, rf.curState)
}

func (rf *Raft) generateRfAppendRPC(heartbeated bool, server int, index int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.Valid = rf.valided[server]
	if heartbeated {
		args.Entries.SetLogs(nil)
	} else {
		rf.nextIndex[server] = Min(rf.log.Len()-1, rf.nextIndex[server])
		// move to left to judge whether right
		args.PrevLogIndex = rf.nextIndex[server]
		args.PrevLogTerm = rf.log.Get(args.PrevLogIndex).Term
		rightIndex := Max(index+1, rf.log.Len()-1)
		args.Entries.SetLogs(rf.log.GetRange(args.PrevLogIndex, rightIndex))
		DPrintf(dLog, "-> S%v generate appendRPC(I%d) Before send(%v) Log: %v at:T%v send:[%v,%v]",
			rf.me, server, index, rf.curState, rf.log, rf.currentTerm, args.PrevLogIndex, rightIndex)

	}
	rf.mu.Unlock()
	return args
}

func (rf *Raft) checkContinue(server int, index int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.curState == Leader && index < rf.log.Len()
}
func (rf *Raft) sendAppnendEntriesUntilWin(server int, heartbeated bool, index int) {
	if heartbeated {
		args := rf.generateRfAppendRPC(heartbeated, server, index)
		// DPrintf(dTrace, "-> S%v Sending Broadcast(%v) args:%v at: T%v",
		// 	rf.me, server, rf.valided[server], &args, rf.currentTerm)
		repl := AppendEntriesRepl{}
		ok := rf.sendAppnendEntries(server, &args, &repl)
		if ok {
			rf.mu.Lock()
			if repl.Term > rf.currentTerm {
				rf.curState = Follower
				for i := range rf.valided {
					rf.valided[i] = false
				}
				DPrintf(dLeader, " Leader to Follower T%v(<T%v(S%v))", rf.me, rf.currentTerm, repl.Term, server)
			}
			rf.mu.Unlock()
		}
	} else {
		for rf.killed() == false && rf.checkContinue(server, index) {
			args := rf.generateRfAppendRPC(heartbeated, server, index)
			repl := AppendEntriesRepl{}
			DPrintf(dTrace, "-> S%v Sending AppendRPC log:%v at:T%v", rf.me, server, rf.log.Len(), rf.currentTerm)
			ok := rf.sendAppnendEntries(server, &args, &repl)
			if ok { // retry until success
				rf.mu.Lock()
				if repl.Term > rf.currentTerm {
					rf.curState = Follower
					for i := range rf.valided {
						rf.valided[i] = false
					}
					DPrintf(dLeader, " Leader to Follower T%v(<T%v(S%v))", rf.me, rf.currentTerm, repl.Term, server)
					rf.mu.Unlock()
					return
				} else if repl.Success == false {
					if rf.nextIndex[server] > 0 {
						rf.nextIndex[server] -= 1
					}
				}
				rf.mu.Unlock()
				if repl.Success {
					rf.mu.Lock()
					if !rf.valided[server] && args.Entries.Len() > 0 {
						DPrintf(dLog, "Set %v commit valid to true args:%v", rf.me, server, &args)
						rf.valided[server] = true
					}
					rf.commitCount[index] += 1
					rf.nextIndex[server] = index + 1
					rf.matchedIndex[server] = index
					rf.mu.Unlock()
					// DPrintf()
					break
				}
			}
			time.Sleep(time.Duration(appendEntriesTetryTimeLap * time.Millisecond))
		}
	}
}

func (rf *Raft) electLeader() bool {
	// ready for election
	rf.mu.Lock()
	rf.curState = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.voteSum = 1
	rf.lastAppendTime = time.Now()
	rf.mu.Unlock()

	// start election
	DPrintf(dVote, "start vote at T%v", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i != rf.me {
			go rf.electLeaderFromVoter(i)
		}
	}
	starttime := time.Now()
	for time.Now().Sub(starttime).Milliseconds() < waitForVoteTimeout {
		rf.mu.Lock()
		if rf.rejectSum > len(rf.peers)/2 || rf.voteSum > len(rf.peers)/2 || rf.curState != Candidate {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(waitForVoteTime * time.Millisecond))
	}
	// check result
	rf.mu.Lock()
	peoSum := len(rf.peers)
	flag := false
	// gain majority vote
	DPrintf(dVote, "vote result:%v/%v", rf.me, rf.voteSum, peoSum)
	if rf.voteSum > peoSum/2 && rf.curState == Candidate {
		rf.curState = Leader
		flag = true
	} else {
		rf.curState = Follower
		for i := range rf.valided {
			rf.valided[i] = false
		}
	}
	rf.voteFor = -1
	rf.mu.Unlock()

	return flag
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

// false : need to start to Elect a new leader
func (rf *Raft) checkElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curState == Leader {
		rf.lastAppendTime = time.Now()
	}
	timeLap := time.Now().Sub(rf.lastAppendTime)
	return rf.curState == Leader || int(timeLap.Milliseconds()) < int(electionTimeout+rand.Int31()%1000) // 500
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

	rand.Seed(time.Now().Unix())
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.voteFor = -1
	rf.curState = Follower
	rf.lastApplied = 0
	rf.commitIndex = -1
	rf.lastAppendTime = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchedIndex = make([]int, len(peers))
	rf.commitCount = map[int]int{}
	rf.applyCh = applyCh
	rf.applyIndex = 0
	rf.lastSnapshotIndex = 0
	rf.log.Offset = 0
	rf.valided = make([]bool, len(peers))
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	DPrintf(dInfo, " start server", rf.me)
	go rf.ticker()

	return rf
}
