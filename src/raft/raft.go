package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// my data structure
//
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
const (
	Follower  = 1
	Candidate = 2
	Leader = 3
)
type State int
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	applyChanel chan ApplyMsg
	heartBeatCh chan int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// state machine
	state State

	// for election
	voteCount int
	// state in Figure 2
	currentTerm int
	voteFor     int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int



}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	DPrintf("--------------------------------------------trying to get %v's state", rf.me)
	rf.mu.Lock()
	defer DPrintf("------------------------------------------get %v's state",rf.me)
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
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
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("%v Receve a request from %v", rf.me, args.CandidateId)
	defer DPrintf("%v completes the processing of %v's RPC", rf.me, args.CandidateId)
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId){
		rf.voteFor = args.CandidateId
		if (len(rf.log)>0){
			myLastLogTerm :=rf.log[len(rf.log)-1].Term
			myLastLogIndex := rf.log[len(rf.log)-1].Index
			reply.VoteGranted = (myLastLogTerm<args.LastLogTerm )|| (myLastLogTerm==args.LastLogTerm && myLastLogIndex<=args.LastLogIndex)
			return
		}
		reply.VoteGranted = true
		return
	}
	DPrintf("%v refuse to vote to %v", rf.me, args.CandidateId)
	reply.VoteGranted = false
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// reply struc
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartBeatCh<-1
	reply.Term = rf.currentTerm
	// rule 1
	if args.Term <rf.currentTerm{
		reply.Success = false
		return
	}

	if rf.state !=Follower{
		rf.state = Follower
	}

	// rule 2
	if (len(args.Entries)==0||len(rf.log)<args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term !=args.PrevLogTerm){
		reply.Success = false
		return
	}

	// TODO :rule 3  & rule 4

	// rule 5
	if args.LeaderCommit >rf.commitIndex{
		if (len(args.Entries)>0){
			if args.Entries[len(args.Entries)-1].Index > args.LeaderCommit{
				rf.commitIndex = args.Entries[len(args.Entries)].Index
			}else{
				rf.commitIndex = args.LeaderCommit
			}
		}else{
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// raft rules
//
func (rf *Raft)sendHeartBeats(){
	rf.mu.Lock()
	copyTerm := rf.currentTerm
	copyCommitIndex := rf.commitIndex
	rf.mu.Unlock()
	for ind := range rf.peers{
		if ind == rf.me	{
			continue
		}
		tem_ind := ind
		go func(){
			args := AppendEntriesArgs{
				copyTerm,
				rf.me,
				0, 0,make([]LogEntry, 0),copyCommitIndex}
			reply:= AppendEntriesReply{
				0, false}
			ok :=rf.sendAppendEntries(tem_ind,  &args, &reply)
			if !ok{
				DPrintf("%v send heart beat to %v fail.", rf.me, tem_ind)
				return
			}
			DPrintf("%v send heart beat to %v success.", rf.me, tem_ind)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != copyTerm || rf.state != Leader {
				return
			}

			if reply.Term > rf.currentTerm{
				rf.currentTerm = reply.Term
				rf.state = Follower
			}
		}()
	}
}

func (rf *Raft)PeriodHeartBeats(){
	for {
		rf.mu.Lock()
		if rf.state != Leader || rf.killed(){
			rf.mu.Unlock()
			return
		}
		DPrintf("%v' dead is %v",rf.me, rf.killed())
		rf.mu.Unlock()
		DPrintf("%v sends the heartbeats", rf.me)
		go rf.sendHeartBeats()
		time.Sleep(time.Millisecond * 1000)
	}
}

func (rf* Raft)Election(){
	rf.mu.Lock()
	// preparation for elction
	rf.currentTerm ++
	rf.state = Candidate
	rf.voteCount = 1
	rf.voteFor = rf.me

	// copy the arguments for RPC
	copyTerm :=rf.currentTerm
	lastLogIndex :=0
	lastLogTerm :=0
	if len(rf.log)>0{
		lastLogIndex = rf.log[len(rf.log)-1].Index
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}else{
		lastLogIndex= 0
		lastLogTerm =0
	}
	DPrintf("%v begins the election, there are %v peers.", rf.me, len(rf.peers))
	rf.mu.Unlock()
	for ind := range rf.peers{
		if ind==rf.me{
			continue
		}

		tem_ind :=ind

		go func(){
			DPrintf("%v send request to %v", rf.me, tem_ind)

			args :=RequestVoteArgs{
				copyTerm,rf.me,lastLogIndex,lastLogTerm,
			}
			reply :=RequestVoteReply{}
			rf.sendRequestVote(tem_ind, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// check the assumptions, if fail, the quit
			if rf.currentTerm !=copyTerm || rf.state !=Candidate{
				DPrintf("%v fails the check", rf.me)
				return
				return
			}

			// check if win the election
			if reply.VoteGranted {
				rf.voteCount ++;
				DPrintf("%v get one count, current vote is %v", rf.me, rf.voteCount)
				if (rf.voteCount > len(rf.peers)/2){
					DPrintf("%v becomes the leader.", rf.me)
					rf.state = Leader
					go rf.PeriodHeartBeats()
				}
				return
			}

			if reply.Term >rf.currentTerm{
				rf.currentTerm = reply.Term
				rf.state = Follower
			}
		}()
	}
}

// election timer
func (rf *Raft)CheckHeartBeats(){
	rand.Seed(time.Now().UnixNano())
	min := 200
	max := 300
	for{
		if rf.killed(){
			DPrintf("%v is killed.", rf.me)
		}
		election_timeout:=rand.Int()*(max-min) + min
		select {
		case <-rf.heartBeatCh:
			DPrintf("%v : get heartBeatCh",rf.me)
			continue
		case<-time.After(time.Millisecond*time.Duration(election_timeout)):// election timeout

			rf.mu.Lock()
			// I'am the leader
			if rf.state == Leader{
				DPrintf("%v is the leader, needn't to begin election.", rf.me)
				rf.mu.Unlock()
				continue
			}
			// timeout and begin new election
			DPrintf("%v : time is out, so begin the election",rf.me)
			rf.voteCount = 0
			go rf.Election()
			rf.mu.Unlock()
		}
	}
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
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

	rf.applyChanel = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 0, 10)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.state = Follower
	rf.dead = 0
	rf.heartBeatCh = make(chan int,1000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.CheckHeartBeats()// start a background goroutine to handle state
	return rf
}

