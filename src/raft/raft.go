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
	Leader    = 3
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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

// ================= RPC =====================

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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		if len(rf.log) > 0 {
			myLastLogTerm := rf.log[len(rf.log)-1].Term
			myLastLogIndex := rf.log[len(rf.log)-1].Index
			reply.VoteGranted = (myLastLogTerm < args.LastLogTerm) || (myLastLogTerm == args.LastLogTerm && myLastLogIndex <= args.LastLogIndex)
			return
		}
		reply.VoteGranted = true
		DPrintf("===RPC=== %v votes %v.", rf.me, args.CandidateId)

		return
	}
	DPrintf("===RPC=== %v refuse to votes %v.", rf.me, args.CandidateId)
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	DPrintf("==HEART BEATS== %v ==> %v.", args.LeaderId, rf.me)
	rf.heartBeatCh <- 1

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// reject the RPC if args.Term is smaller
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// args.Term is at least as large as currentTerm, so convert to Follower
	if rf.state != Follower {
		rf.state = Follower
	}

	// if no LogEntry matches, return false
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	for ind, log:= range args.Entries{
		curIndex := args.Entries[ind].Index
		// don't need to compare, append for once
		if len(rf.log)<=curIndex{
			rf.log = append(rf.log, args.Entries[ind:]...)
			break
		}

		// delete all the logs that mismatch
		if rf.log[curIndex].Term != log.Term{
			rf.log = rf.log[:curIndex]
			rf.log = append(rf.log, args.Entries[ind:]...)
			break
		}

		rf.log[curIndex] = log
	}

	// update commitIndex
	max := func(x int, y int)int{
		if x>y{
			return x
		}
		return y
	}
	if args.LeaderCommit > rf.commitIndex {
		if len(args.Entries) > 0 {
			rf.commitIndex = max(args.LeaderId, len(rf.log)-1)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ================= RPC =====================

// ================= Heart Beats =============

//
// raft rules
//
func (rf *Raft) sendHeartBeats() {
	rf.mu.Lock()
	copyTerm := rf.currentTerm
	copyCommitIndex := rf.commitIndex
	logLen := len(rf.log)
	copyPrevLogIndex := logLen-1
	copyPrevLogTerm :=rf.log[logLen-1].Term
	rf.mu.Unlock()
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}
		temInd := ind
		go func() {
			args := AppendEntriesArgs{
				copyTerm,
				rf.me,
				copyPrevLogIndex, copyPrevLogTerm, make([]LogEntry, 0), copyCommitIndex}
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(temInd, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// check the assumptions
			if rf.currentTerm != copyTerm || rf.state != Leader {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
			}
		}()
	}
}

//
// Leader send periodic heart beats
//
func (rf *Raft) PeriodHeartBeats() {
	for {
		rf.mu.Lock()
		// quit if no more alive or a Leader
		if rf.state != Leader || rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		go rf.sendHeartBeats()
		time.Sleep(time.Millisecond * 100)
	}
}

// ================= Heart Beats =============

// ================ Election =================


//
// invoked when raft node's election time elapses
// do preparations and send RPC to all peers
//
func (rf *Raft) Election() {
	// preparation for elction
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.voteCount = 1
	rf.voteFor = rf.me
	DPrintf("==ELECTION BEGIN== %v begins, term is %v.", rf.me, rf.currentTerm)
	// copy the arguments for RPC
	copyTerm := rf.currentTerm
	logLen := len(rf.log)
	lastLogIndex := rf.log[logLen-1].Index
	lastLogTerm := rf.log[logLen-1].Term
	rf.mu.Unlock()

	// send RPC to all peers
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}

		temInd := ind

		go func() {
			args := RequestVoteArgs{
				copyTerm, rf.me, lastLogIndex, lastLogTerm,
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(temInd, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// check the assumptions, if fail, the quit
			if rf.currentTerm != copyTerm || rf.state != Candidate {
				return
			}

			// check if win the election
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					DPrintf("==ELECTION== %v becomes the leader.", rf.me)
					// init the Leader states
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for ind := range rf.nextIndex{
						rf.nextIndex[ind] = len(rf.log)
					}
					for ind := range rf.matchIndex{
						rf.matchIndex[ind] = 0
					}

					go rf.PeriodHeartBeats()
				}
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
			}
		}()
	}
}

// election timer
func getRandElectionTimeout(min int, max int) func() int {
	rand.Seed(time.Now().UnixNano())
	return func() int {
		return rand.Int()%(max-min) + min
	}
}

func (rf *Raft) CheckHeartBeats() {
	randElectionTimeout := getRandElectionTimeout(300, 500)
	for {
		if rf.killed() {
			DPrintf("%v is killed.", rf.me)
			return
		}

		electionTimeout := randElectionTimeout()
		select {
		case <-rf.heartBeatCh:
			continue
		case <-time.After(time.Millisecond * time.Duration(electionTimeout)): // election timeout
			// skip if as a leader
			rf.mu.Lock()
			if rf.state == Leader {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			// timeout and begin new election
			go rf.Election()
		}
	}
}

// ================ Election =================

// ================ Agreemnet ================

// update the CommitIndex in O(log (len))
func (rf *Raft) updateCommitIndex(){
	check := func (val int) bool {
		count :=0
		for ind := range rf.peers{
			if ind == rf.me{
				continue
			}

			if rf.matchIndex[ind]>=val{
				count++
			}
		}
		return count >= len(rf.peers)/2
	}

	rf.mu.Lock()
	if rf.state != Leader{
		rf.mu.Unlock()
		return
	}
	lhs:=0
	rhs:=len(rf.log)
	for lhs < rhs{
		mid := (lhs+rhs+1)>>1
		if check(mid){
			DPrintf("==CHECK== %v succ.",mid)
			lhs = mid
		}else{
			DPrintf("==CHECK== %v fail.",mid)
			rhs = mid-1
		}
	}

	DPrintf("==AGREEMENT== %v -> %v", rf.commitIndex, lhs)
	rf.commitIndex = lhs
	rf.mu.Unlock()
}

func (rf *Raft) getAgreement(){
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}
		temInd := ind
		// update in inner, for these values my change
		rf.mu.Lock()
		copyTerm := rf.currentTerm
		copyCommitIndex := rf.commitIndex
		copyPrevLogIndex := rf.nextIndex[temInd]-1
		copyPrevLogTerm :=rf.log[copyPrevLogIndex].Term
		// get the Entries
		targetLog := rf.log[rf.nextIndex[temInd]:]
		copyLog := make([]LogEntry, len(targetLog))
		copy(copyLog, targetLog)
		rf.mu.Unlock()
		go func() {
			for{
				args := AppendEntriesArgs{
					copyTerm,
					rf.me,
					copyPrevLogIndex, copyPrevLogTerm, copyLog, copyCommitIndex}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(temInd, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				//defer rf.mu.Unlock()

				// check the assumptions
				if rf.currentTerm != copyTerm || rf.state != Leader {
					DPrintf("==AGREEMENT== fail check, prev term: %v, current term:%v", copyTerm, rf.currentTerm)
					rf.mu.Unlock()
					return
				}

				if reply.Success{
					DPrintf("==AGREEMENT== %v -> %v o.[%v, %v -> %v, %v]", rf.me, temInd, rf.nextIndex[temInd], rf.matchIndex[temInd], len(rf.log), len(rf.log)-1)
					rf.nextIndex[temInd] = len(rf.log)
					rf.matchIndex[temInd] = len(rf.log)-1
					rf.mu.Unlock()
					rf.updateCommitIndex()
					return
				}

				DPrintf("==AGREEMENT== %v -> %v x.", rf.me, temInd)
				// fail for the out of date
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.mu.Unlock()
					return
				}

				// updates the args
				rf.nextIndex[temInd]-=1
				DPrintf("==AGREEMENT== %v x.[%v %v -> %v %v]", temInd, copyPrevLogIndex, copyPrevLogTerm, copyPrevLogIndex-1, rf.log[copyPrevLogIndex-1].Term)
				copyPrevLogIndex --
				copyPrevLogTerm= rf.log[copyPrevLogIndex].Term

				targetLog := rf.log[rf.nextIndex[temInd]:]
				copyLog = make([]LogEntry, len(targetLog))
				copy(copyLog, targetLog)
				rf.mu.Unlock()
			}

		}()
	}
}

//
// background thread to send notify to the channel
//
func (rf *Raft) notifyCommit(){
	lastNotify :=0
	for{
		//DPrintf("==AGREEMENT== check %v's commit", rf.me)
		rf.mu.Lock()
		if rf.killed(){
			return
		}

		if rf.commitIndex>lastNotify{
			for ind:= lastNotify+1;ind<=rf.commitIndex;ind++{
				DPrintf("==AGREEMENT== %v commits %v.",rf.me, ind)
				rf.applyChanel<-ApplyMsg{true,rf.log[ind].Command, ind}
			}
			lastNotify = rf.commitIndex
		}
		rf.mu.Unlock()
		time.Sleep(100*time.Millisecond)
	}
}
// ================ Agreemnet ================

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := !rf.killed() && (rf.state== Leader)

	// if not a Leader, return immediately
	if !isLeader{
		return index, term, isLeader
	}

	// start agreement on new command
	rf.log = append(rf.log, LogEntry{term, index, command})
	go rf.getAgreement()


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
	rf.heartBeatCh = make(chan int, 10)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// log[0] serve as a pioneer
	rf.log = append(rf.log, LogEntry{0,0,nil})

	// background goroutine to monitor heart beats, begin election
	go rf.CheckHeartBeats()
	go rf.notifyCommit()
	return rf
}
