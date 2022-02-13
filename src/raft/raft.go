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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
const heartbeatTimeLap = 500
const electionTimeout = 2000
const sleepLapTimeBeforeElection = 1000
const appendEntriesTetryTimeLap = 500

//
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

	log []Log

	commitIndex int
	lastApplied int

	nextIndex    []int
	matchedIndex []int

	lastAppendTime time.Time
}

type Log struct {
	Term    int
	Command interface{}
}

// get Log last item's index and term
func (rf *Raft) GetLastItemInfo() (int, int) {
	l := len(rf.log)
	if l == 0 {
		return 0, 0
	} else {
		return l - 1, rf.log[l-1].Term
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	// TODO - whether this server believes it is the leader. ???
	isleader := (rf.curState == Leader)
	// Your code here (2A).
	return term, isleader
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
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
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

	fmt.Println(rf.me, "recv heartbeat from", args.LeaderId)
	rf.lastAppendTime = time.Now()
	if args.Entries != nil {

	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.VoteGranted = false
	} else if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		lastIndex, lastTerm := rf.GetLastItemInfo()
		if lastTerm > args.LastLogTerm {
			reply.VoteGranted = false
		} else if lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex {
			reply.VoteGranted = false
		} else {
			reply.VoteGranted = true
			rf.voteFor = args.CandidateId
		}
	}
	fmt.Println(rf.me, "vote for", rf.voteFor)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
			sleepTime := random(sleepLapTimeBeforeElection/2, sleepLapTimeBeforeElection)
			fmt.Println("sleep time: ", sleepTime)
			time.Sleep(time.Duration(sleepTime * int(time.Millisecond)))

			if rf.checkElection() {
				continue
			}

			fmt.Println("Start elect", time.Now())
			if rf.electLeader() {
				rf.mu.Lock()
				lastLogIndex := len(rf.log)
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
				}
				rf.mu.Unlock()
				// broadcast immediate prevent others become leader

				fmt.Println(rf.me, "become leader")
				go rf.heartbeat()
			}
		}
	}
	fmt.Println(rf.me, "be killed")
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendAppnendEntriesUntilWin(i, nil, true)
			}
		}
		time.Sleep(time.Duration(heartbeatTimeLap * time.Millisecond))
	}
	fmt.Println(rf.me, "heartbeat be killed")
}

func (rf *Raft) sendAppnendEntriesUntilWin(server int, entries []Log, heartbeated bool) {
	if heartbeated {
		args := rf.generateRfAppendRPC(entries, heartbeated)
		repl := AppendEntriesRepl{}
		ok := rf.sendAppnendEntries(server, &args, &repl)
		if ok {
			// TO-DO: do something
		}
	} else {
		for rf.killed() == false {
			args := rf.generateRfAppendRPC(entries, heartbeated)
			repl := AppendEntriesRepl{}
			ok := rf.sendAppnendEntries(server, &args, &repl)
			if ok && repl.Success { // retry until success
				break
			}
			time.Sleep(time.Duration(appendEntriesTetryTimeLap * time.Millisecond))
		}
	}
}

func (rf *Raft) generateRfAppendRPC(entries []Log, heartbeated bool) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	if heartbeated {

	} else {

	}
	// TO-DO: set preLogIndex, peovLogTerm, entries[]
	rf.mu.Unlock()
	return args
}

func (rf *Raft) electLeader() bool {
	// ready for election
	rf.mu.Lock()
	rf.curState = Candidate
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.voteSum = 1
	rf.mu.Unlock()

	// start election
	for i := range rf.peers {
		if i != rf.me {
			fmt.Println(rf.me, " Send vote message to", i)
			go rf.electLeaderFromVoter(i)
		}
	}
	time.Sleep(time.Duration((electionTimeout / 2) * int(time.Millisecond)))
	// check result
	rf.mu.Lock()
	peoSum := len(rf.peers)
	flag := false
	// gain majority vote
	if rf.voteSum > peoSum/2 {
		rf.curState = Leader
		flag = true
	} else {
		rf.curState = Follower
	}
	rf.voteFor = -1
	rf.mu.Unlock()

	return flag
}

func (rf *Raft) electLeaderFromVoter(voter int) {
	args := RequestVoteArgs{}
	rf.mu.Lock()
	args.CandidateId = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex, args.LastLogTerm = rf.GetLastItemInfo()
	rf.mu.Unlock()
	repl := RequestVoteReply{}
	ok := rf.sendRequestVote(voter, &args, &repl)
	if ok && repl.VoteGranted {
		fmt.Println(voter, "successfully vote for", rf.me)
		rf.mu.Lock()
		rf.voteSum += 1
		rf.mu.Unlock()
	} else {
		fmt.Println(voter, "failed vote for", rf.me)
		rf.mu.Lock()
		if repl.Term > rf.currentTerm {
			rf.currentTerm = repl.Term
		}
		rf.mu.Unlock()
	}
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

// false : need to start to Elect a new leader
func (rf *Raft) checkElection() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	timeLap := time.Now().Sub(rf.lastAppendTime)
	fmt.Println(rf.me, "not election time out", int(timeLap.Milliseconds()))
	return rf.curState == Leader || int(timeLap.Milliseconds()) < electionTimeout
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
	rf.commitIndex = 0
	rf.lastAppendTime = time.Now()
	rf.nextIndex = make([]int, len(peers))
	rf.matchedIndex = make([]int, len(peers))
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	fmt.Println("Start tick")
	go rf.ticker()

	return rf
}
