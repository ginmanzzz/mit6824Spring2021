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
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
	"time"
	"math/rand"
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

type LogEntry struct {
	Command interface{}
	Term int
}

type RaftRole int
const (
	RaftCandidate RaftRole = iota
	RaftFollower
	RaftLeader
)

const (
	leaderHeartbeatTime time.Duration = 100 * time.Millisecond
	minElectionTimeout time.Duration = 800 * time.Millisecond
	maxElectionTimeout time.Duration = 1000 * time.Millisecond
	maxRPCTimeout time.Duration = 100 * time.Millisecond
)

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
	currentTerm int
	votedFor int // -1 is none
	logs []LogEntry // first index is 1

	// volatile state
	commitIndex int // the known highest index of committed log entry, initialized with 0
	lastApplied int // applied to state machine, initialized with 0
	role RaftRole
	
	// volatile leader state
	nextIndex []int // the index of next log entry to send
	matchIndex []int // the highest index of log entry known to be replicated
	
	// other
	lastHeartbeat time.Time
}

func (rf *Raft) logTerm(index int) int {
	if index == 0 {
		return 0
	}
	return rf.logs[index - 1].Term
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.logs) != 0 {
		return rf.logs[len(rf.logs) - 1].Term
	}
	return 0
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs)
}

func (rf *Raft) getLogsStartFrom(index int) []LogEntry {
	if index == 0 {
		return []LogEntry{}
	}
	return rf.logs[index - 1:]
}

func (rf *Raft) dropLogsFrom(index int) {
	if index == 0 {
		DPrintf("server %d: can't drop index 0 log, it's virtual!\n", rf.me)
		panic(1)
	}
	rf.logs = rf.logs[:index - 1]
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == RaftLeader
	// return term, isleader
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate term
	CandidateId int 
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // currentTerm, for candidate to update itself
	VoteGrand bool // true means candidate recevied vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGrand = false
	if rf.currentTerm > args.Term {
		DPrintf("server %d: rejected candidate<%d>, term %d > candidate's term: %d.",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	} else if rf.lastLogTerm() > args.LastLogTerm {
		DPrintf("server %d: rejected candidate<%d>, last log term %d > candidate's last log term: %d.",
			rf.me, args.CandidateId, rf.lastLogTerm(), args.Term)
		return
	} else if rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex {
		DPrintf("server %d: rejected candidate<%d>, lastLogIndex %d > candidate's last log index: %d.",
			rf.me, args.CandidateId, rf.lastLogIndex(), args.LastLogIndex)
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = RaftFollower
		rf.lastHeartbeat = time.Now()
		DPrintf("server %d updated term %d. retrieved vote.", rf.me, rf.currentTerm)
	}
	if rf.votedFor != -1 {
		DPrintf("server %d have voted, giving up voting to candidate %d.", rf.me, args.CandidateId)
		return
	}
	reply.VoteGrand = true
	rf.votedFor = args.CandidateId
	DPrintf("server %d: voted candidate<%d>.", rf.me, args.CandidateId)
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
		rf.mu.Lock()
		preHeartbeat := rf.lastHeartbeat
		rf.mu.Unlock()
		time.Sleep(getRandomElectionTime())
		rf.mu.Lock()
		role := rf.role
		heartbeat := int(rf.lastHeartbeat.Sub(preHeartbeat))
		rf.mu.Unlock()
		if role == RaftLeader {
			DPrintf("server %d: I'm leader, election timeout skipped.\n", rf.me)
			continue
		}
		if heartbeat <= 0 {
			DPrintf("server %d: election timeout, starting election.", rf.me)
			rf.electOnce()
		}
	}
}

func (rf *Raft) electOnce() {
	rf.mu.Lock()
	rf.role = RaftCandidate
	DPrintf("server %d: starting election.\n", rf.me)
	rf.currentTerm++
	rf.votedFor = rf.me
	DPrintf("server %d: Term: %d, voted its self.\n", rf.me, rf.currentTerm)
	ch := make(chan int, len(rf.peers))
	args := RequestVoteArgs {
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm: rf.lastLogTerm(),
	}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			req := args
			rsp := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &req, &rsp)
			if !ok {
				ch <- 0
			}
			if rsp.Term > req.Term {
				ch <- -1
			} else if rsp.VoteGrand {
				ch <- 1
			} else {
				ch <- 0
			}
		}(i)
	}
	go func() {
		time.Sleep(maxRPCTimeout)
		ch <- -100
	}()
	rf.mu.Lock()
	if rf.currentTerm != args.Term || rf.role != RaftCandidate {
		return
	}
	rf.mu.Unlock()
	votes := 1
	for i := 0; i < len(rf.peers) - 1; i++ {
		vote := <-ch
		if vote == -1 {
			rf.mu.Lock()
			rf.role = RaftFollower
			rf.votedFor = -1
			DPrintf("server %d: give up election, become follower, retrieved vote.\n", rf.me)
			rf.mu.Unlock()
			return
		} else if vote == 1 {
			votes++
			if votes > len(rf.peers) / 2 {
				rf.mu.Lock()
				rf.role = RaftLeader
				go rf.leaderTicker()
				DPrintf("server %d: I have become leader.\n", rf.me)
				rf.mu.Unlock()
				return
			}
		} else if vote == -100 {
			rf.mu.Lock()
			DPrintf("server %d: vote rpc timeout. server %d and later server all failed.\n", rf.me, i)
			rf.mu.Unlock()
			return
		}
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = RaftFollower

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.lastHeartbeat = time.Now()
	DPrintf("making raft server.")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

type AppendEntriesArgs struct {
	Term int // leader's term
	LeaderId int
	PrevLogIndex  int // index of log entry immediately preceding new ones
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// hanlder of AppendEntries
// Need to reset the election timeout
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		DPrintf("server %d: refused leader %d AppendEntries request, leader's term %d < its term %d.",
			rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = RaftFollower
		DPrintf("server %d updated term %d. retrieved vote.", rf.me, rf.currentTerm)
	}
	// reset timeout
	rf.lastHeartbeat = time.Now()
	if args.PrevLogIndex > rf.lastLogIndex() {
		DPrintf("server %d: refused leader %d AppendEntries request, leader's prevLogIndex %d > its lastLogIndex%d.",
			rf.me, args.LeaderId, args.PrevLogIndex, rf.lastLogIndex())
		return
	}
	if args.PrevLogTerm != rf.logTerm(args.PrevLogIndex) {
		DPrintf("server %d: refused leader %d AppendEntries request, leader's prevLogTerm %d != its corresponding term %d.",
			rf.me, args.LeaderId, args.PrevLogTerm, rf.logTerm(args.PrevLogIndex))
		return
	}
	reply.Success = true
	rf.dropLogsFrom(args.PrevLogIndex + 1)
	rf.logs = append(rf.logs, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.lastLogIndex())
	}
	DPrintf("server %d: leader %d append successfully.", rf.me, args.LeaderId)
}

func getRandomElectionTime() time.Duration {
		randomTime := minElectionTimeout + time.Duration(rand.Int63n(int64((maxElectionTimeout - minElectionTimeout) / time.Millisecond))) * time.Millisecond
		return randomTime
}


type leaderAppendEntriesLog struct {
	Sent bool
	Request AppendEntriesArgs
	Result AppendEntriesReply
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		time.Sleep(leaderHeartbeatTime)
		rf.mu.Lock()
		if rf.role != RaftLeader {
			rf.mu.Unlock()
			break
		}
		DPrintf("server %d: leader ticking...\n", rf.me)
		appendEntriesLogs := make([]leaderAppendEntriesLog, len(rf.peers))
		waitCh := make(chan int)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			appendEntriesLogs[i].Sent = false
			appendEntriesLogs[i].Request = AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.matchIndex[i],
				PrevLogTerm: rf.logTerm(rf.matchIndex[i]),
				Entries: rf.getLogsStartFrom(rf.nextIndex[i]),
				LeaderCommit: rf.commitIndex,
			}
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				DPrintf("server %d: sending AppendEntries to server %d.\n", rf.me, i)
				ok := rf.sendAppendEntries(i, &appendEntriesLogs[i].Request, &appendEntriesLogs[i].Result)
				if ok {
					appendEntriesLogs[i].Sent = true
					DPrintf("server %d: sent AppendEntries to server %d.\n", rf.me, i)
				} else {
					DPrintf("server %d: can't send AppendEntries to server %d.\n", rf.me, i)
				}
				waitCh <- i
			}(i)
		}
		go func () {
			// rpc timeout monitor
			time.Sleep(maxRPCTimeout)
			waitCh <- -1
		}()
		for j := 0; j < len(rf.peers) - 1; j++ {
			i := <- waitCh
			rf.mu.Lock()
			if i < 0 {
				DPrintf("server %d: server %d and later append failed, rpc timeout.\n", rf.me, j)
				rf.mu.Unlock()
				break
			}
			if !appendEntriesLogs[i].Sent {
				rf.mu.Unlock()
				continue
			}
			if appendEntriesLogs[i].Result.Term > rf.currentTerm {
				rf.currentTerm = appendEntriesLogs[i].Result.Term
				rf.role = RaftFollower
				rf.votedFor = -1
				rf.mu.Unlock()
				DPrintf("server %d: give up leading, become follower, retrieved vote.\n", rf.me)
				break
			}
			if !appendEntriesLogs[i].Result.Success {
				rf.matchIndex[i] = maxInt(rf.matchIndex[i], 0)
				rf.nextIndex[i] = maxInt(rf.nextIndex[i], 1)
				DPrintf("server %d: append server %d failed. its matchIndex: %d, its nextIndex: %d.\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i])
			} else {
				rf.matchIndex[i] = appendEntriesLogs[i].Request.PrevLogIndex + len(appendEntriesLogs[i].Request.Entries)
				rf.nextIndex[i] = rf.matchIndex[i] + 1
				DPrintf("server %d: append server %d successfully. its matchIndex: %d, its nextIndex: %d.\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i])
			}
			rf.mu.Unlock()
		}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
