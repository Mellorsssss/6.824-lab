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
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

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

//
// my data structure
//
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	Follower  = 1
	Candidate = 2
	Leader    = 3
)
type State int

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
	CurrentTerm int
	VoteFor     int
	Log         []LogEntry
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
	return rf.CurrentTerm, rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	DPrintf("==PERSIST== %v backs up.", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		panic("==PERSIST== no such value.")
	} else {
		rf.CurrentTerm = currentTerm
		rf.VoteFor = voteFor
		rf.Log = logs
	}
}

// ================= UTILS ===================
func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

// you should make sure you have lock outside
func (rf *Raft) findFirstLogInTerm(Term int) (int, bool) {
	lhs := 1
	rhs := len(rf.Log)
	for lhs < rhs {
		mid := (lhs + rhs) >> 1
		if rf.Log[mid].Term >= Term {
			//DPrintf("==AGREEMENT== %v's term is %v,success", mid, rf.Log[mid].Term)
			rhs = mid
		} else {
			//DPrintf("==AGREEMENT== %v's term is %v,fail", mid, rf.Log[mid].Term)
			lhs = mid + 1
		}
	}
	if lhs == len(rf.Log) {
		return lhs, false
	}

	if rf.Log[lhs].Term != Term {
		return lhs, false
	}

	return lhs, true
}

// you should make sure you have lock outside
func (rf *Raft) findLastLogInTerm(Term int) (int, bool) {
	lhs := 0
	rhs := len(rf.Log) - 1
	for lhs < rhs {
		mid := (lhs + rhs + 1) >> 1
		if rf.Log[mid].Term <= Term {
			lhs = mid
		} else {
			rhs = mid - 1
		}
	}
	if lhs == len(rf.Log)-1 {
		return lhs, false
	}

	if rf.Log[lhs].Term != Term {
		return lhs, false
	}

	return lhs, true
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
	reply.Term = rf.CurrentTerm

	// reject the RPC
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		return
	}

	// convert to Follower
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		rf.state = Follower
		rf.persist()
	}

	// only if rf hasn't vote in this Term
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		rf.VoteFor = args.CandidateId
		rf.persist()

		// check if the candidate's log is as-up-to-date as rf's
		if len(rf.Log) == 0 {
			DPrintf("===================FUCKING MISTAKE!==============")
		}
		myLastLogTerm := rf.Log[len(rf.Log)-1].Term
		myLastLogIndex := rf.Log[len(rf.Log)-1].Index
		reply.VoteGranted = (myLastLogTerm < args.LastLogTerm) || (myLastLogTerm == args.LastLogTerm && myLastLogIndex <= args.LastLogIndex)
		// reset election timer if vote granted
		if reply.VoteGranted {
			rf.heartBeatCh <- 1
		} else {
			DPrintf("==ELECTION== %v deny %v", rf.me, args.CandidateId)
		}
		return
	}
	//DPrintf("===RPC=== %v refuse to votes %v.", rf.me, args.CandidateId)
	reply.VoteGranted = false
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
	Term          int
	Success       bool
	ConflictTerm  int // if true, then MatchIndex indicates the first index in that term
	ConflictIndex int
	ParameValid bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// prepare the args
	reply.Term = rf.CurrentTerm
	reply.ParameValid = false

	// reject the RPC if args.Term is smaller
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	DPrintf("==AE IN== %v ==> %v at %v with len %v from %v", args.LeaderId, rf.me, args.Term, len(args.Entries), args.PrevLogIndex+1)

	if rf.state!=Leader{
		rf.heartBeatCh <- 1
	}

	// args.Term is at least as large as CurrentTerm, so convert to Follower
	if args.Term > rf.CurrentTerm {
		DPrintf("==STATE== %v [%v] -> Follower [%v]", rf.me, rf.CurrentTerm, args.Term)
		rf.CurrentTerm = args.Term
		rf.persist()
		rf.state = Follower
	}

	reply.ParameValid = true
	// if no LogEntry matches, return false
	if len(rf.Log) <= args.PrevLogIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.Log)
		return
	}

	if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// if there is such a index, then try to find the first Entry in that term
		// use the binary search

		reply.Success = false
		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
		var ok bool
		reply.ConflictIndex, ok = rf.findFirstLogInTerm(reply.ConflictTerm)
		if !ok {
			DPrintf("================ERROR: no such value!!!!!==================")
		}
		return
	}

	lastNewEntryIndex := max(args.PrevLogIndex, rf.commitIndex)

	// append new entries
	if len(args.Entries) > 0 {
		for ind, log := range args.Entries {
			curIndex := args.Entries[ind].Index
			// don't need to compare, append for once
			if len(rf.Log) <= curIndex {
				rf.Log = append(rf.Log, args.Entries[ind:]...)
				rf.persist()
				lastNewEntryIndex = args.Entries[len(args.Entries)-1].Index
				break
			}

			// delete all the logs that mismatch
			if rf.Log[curIndex].Term != log.Term {
				if  curIndex<= rf.commitIndex{
					DPrintf("==DEBUG== %v < commitIndex %v, just return", ind, rf.commitIndex)
					return
				}
				rf.Log = rf.Log[:curIndex]
				DPrintf("==DEBUG== truncate %v", rf.me)
				rf.Log = append(rf.Log, args.Entries[ind:]...)
				lastNewEntryIndex = args.Entries[len(args.Entries)-1].Index
				rf.persist()
				break
			}

			//rf.Log[curIndex] = log
			DPrintf("==AE IN== %v assign %v's %v", args.LeaderId, rf.me, curIndex)
		}
		//}
	}

	// update the commitIndex
	if args.LeaderCommit > rf.commitIndex {
		//if len(args.Entries) > 0 {
		//	lastNewEntryIndex := args.Entries[len(args.Entries)-1].Index
		//	rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		//} else {
		//	rf.commitIndex = min(args.LeaderCommit, max(args.PrevLogIndex, rf.commitIndex))
		//}
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// ================= Heart Beats =============

//
// raft rules
//
func (rf *Raft) sendHeartBeats() {
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}
		temInd := ind
		// update in inner, for these values my change
		rf.mu.Lock()
		copyTerm := rf.CurrentTerm
		copyCommitIndex := rf.commitIndex
		copyPrevLogIndex := rf.nextIndex[temInd] - 1
		copyPrevLogTerm := rf.Log[copyPrevLogIndex].Term
		// get the Entries

		var targetLog []LogEntry
		var copyLog []LogEntry
		if rf.Log[len(rf.Log)-1].Term == rf.CurrentTerm && len(rf.Log)>rf.nextIndex[temInd]{
			DPrintf("==HEART BEAT== %v has logs from %v to %v to send to %v.", rf.me, rf.nextIndex[temInd], len(rf.Log)-1,temInd)
			targetLog = rf.Log[rf.nextIndex[temInd]:]
		}else{
			DPrintf("==HEART BEAT== %v send heart beat to %v.", rf.me, temInd)
			targetLog = make([]LogEntry, 0)
		}

		copyLog = make([]LogEntry, len(targetLog))
		copy(copyLog, targetLog)
		rf.mu.Unlock()
		go func() {
			for {
				args := AppendEntriesArgs{
					copyTerm,
					rf.me,
					copyPrevLogIndex, copyPrevLogTerm, copyLog, copyCommitIndex}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(temInd, &args, &reply)
				rf.mu.Lock()

				// check the assumptions
				if rf.killed() || rf.CurrentTerm != copyTerm || rf.state != Leader  || rf.Log[copyPrevLogIndex].Term != copyPrevLogTerm||rf.nextIndex[temInd] != copyPrevLogIndex+1 {
					DPrintf("==AE REPLY== fail check")
					rf.mu.Unlock()
					return
				}

				if !ok {
					rf.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if reply.Success {
					DPrintf("==AE REPLY== %v -> %v success.next: %v -> %v, match : %v -> %v.", rf.me, temInd, rf.nextIndex[temInd], args.PrevLogIndex+len(args.Entries)+1, rf.matchIndex[temInd], args.PrevLogIndex+len(args.Entries))
					rf.nextIndex[temInd] = args.PrevLogIndex + len(args.Entries) + 1 //len(rf.Log)
					rf.matchIndex[temInd] = args.PrevLogIndex + len(args.Entries)    //len(rf.Log) - 1
					rf.mu.Unlock()
					rf.updateCommitIndex()
					return
				}

				// fail for the out of date
				if reply.Term > rf.CurrentTerm {
					DPrintf("==AE REPLY== %v get bigger term", rf.me)
					rf.CurrentTerm = reply.Term
					rf.state = Follower
					rf.persist()
					rf.mu.Unlock()
					return
				}

				lastEntry, ok := rf.findLastLogInTerm(reply.ConflictTerm)
				if ok {
					rf.nextIndex[temInd] = lastEntry + 1
					DPrintf("==AE REPLY==%v -> %v  fail.nextIndex %v : %v -> %v", rf.me, temInd, temInd, args.PrevLogIndex+1, rf.nextIndex[temInd])
				} else {
					rf.nextIndex[temInd] = reply.ConflictIndex
					DPrintf("==AE REPLY==%v -> %v fail.nextIndex %v : %v -> %v", rf.me, temInd, temInd, args.PrevLogIndex+1, rf.nextIndex[temInd])
				}

				// prepare the args for next RPC
				copyPrevLogIndex = rf.nextIndex[temInd] - 1
				copyPrevLogTerm = rf.Log[copyPrevLogIndex].Term

				targetLog := rf.Log[rf.nextIndex[temInd]:]
				copyLog = make([]LogEntry, len(targetLog))
				copy(copyLog, targetLog)
				rf.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
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
		time.Sleep(time.Millisecond * 90)
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
	rf.CurrentTerm++
	rf.state = Candidate
	rf.voteCount = 1
	rf.VoteFor = rf.me
	rf.persist()

	DPrintf("==ELECTION BEGIN== %v begins, term is %v.", rf.me, rf.CurrentTerm)
	// copy the arguments for RPC
	copyTerm := rf.CurrentTerm
	logLen := len(rf.Log)
	lastLogIndex := rf.Log[logLen-1].Index
	lastLogTerm := rf.Log[logLen-1].Term

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
			if rf.CurrentTerm != copyTerm || rf.state != Candidate {
				return
			}

			// check if win the election
			if reply.VoteGranted {
				rf.voteCount++
				DPrintf("==ELECTION== %v get %v's vote, has %v votes now.", rf.me, temInd, rf.voteCount)
				if rf.voteCount > len(rf.peers)/2 {
					DPrintf("==ELECTION== %v becomes the leader.", rf.me)
					// init the Leader states
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for ind := range rf.nextIndex {
						rf.nextIndex[ind] = len(rf.Log)
					}
					for ind := range rf.matchIndex {
						rf.matchIndex[ind] = 0
					}

					go rf.PeriodHeartBeats()
				}
				return
			}

			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.state = Follower
				rf.persist()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	randElectionTimeout := getRandElectionTimeout(300, 400) //300 500
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

// ================ Agreement ================

// update the CommitIndex in O(Log (len))
func (rf *Raft) updateCommitIndex() {
	check := func(val int) bool {
		count := 0
		for ind := range rf.peers {
			if ind == rf.me {
				continue
			}

			if rf.matchIndex[ind] >= val {
				count++
			}
		}
		return count >= len(rf.peers)/2
	}

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	lhs := 0
	rhs := len(rf.Log)
	for lhs < rhs {
		mid := (lhs + rhs + 1) >> 1
		if check(mid) {
			//DPrintf("==CHECK== %v succ.",mid)
			lhs = mid
		} else {
			//DPrintf("==CHECK== %v fail.",mid)
			rhs = mid - 1
		}
	}

	if lhs > rf.commitIndex && rf.Log[lhs].Term == rf.CurrentTerm {
		DPrintf("==COMMIT== %v -> %v", rf.commitIndex, lhs)
		rf.commitIndex = lhs
	}
	rf.mu.Unlock()
}

func (rf *Raft) getAgreement() {
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}
		temInd := ind
		// update in inner, for these values my change
		rf.mu.Lock()
		copyTerm := rf.CurrentTerm
		copyCommitIndex := rf.commitIndex
		copyPrevLogIndex := rf.nextIndex[temInd] - 1
		copyPrevLogTerm := rf.Log[copyPrevLogIndex].Term
		// get the Entries
		targetLog := rf.Log[rf.nextIndex[temInd]:]
		copyLog := make([]LogEntry, len(targetLog))
		copy(copyLog, targetLog)
		rf.mu.Unlock()
		go func() {
			for {
				args := AppendEntriesArgs{
					copyTerm,
					rf.me,
					copyPrevLogIndex, copyPrevLogTerm, copyLog, copyCommitIndex}
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(temInd, &args, &reply)
				rf.mu.Lock()

				// check the assumptions
				if rf.killed() || rf.CurrentTerm != copyTerm || rf.state != Leader  || rf.Log[copyPrevLogIndex].Term != copyPrevLogTerm {
					DPrintf("==AE REPLY== fail check")
					rf.mu.Unlock()
					return
				}

				// re-send the quest
				if rf.nextIndex[temInd] != copyPrevLogIndex+1 &&len(args.Entries)>0 &&args.Entries[len(args.Entries)-1].Index > rf.matchIndex[temInd]{
					// prepare the args for next RPC
					DPrintf("==AE REPLY== %v -> %v resend.PreLogIndex from %v to %v.", rf.me, temInd, copyPrevLogIndex, rf.nextIndex[temInd]-1)
					copyPrevLogIndex = rf.nextIndex[temInd] - 1
					copyPrevLogTerm = rf.Log[copyPrevLogIndex].Term

					targetLog := rf.Log[rf.nextIndex[temInd]:]
					copyLog = make([]LogEntry, len(targetLog))
					copy(copyLog, targetLog)
					rf.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if !ok {
					rf.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
					continue
				}

				if reply.Success {
					DPrintf("==AE REPLY== %v -> %v success.next: %v -> %v, match : %v -> %v.", rf.me, temInd, rf.nextIndex[temInd], args.PrevLogIndex+len(args.Entries)+1, rf.matchIndex[temInd], args.PrevLogIndex+len(args.Entries))
					rf.nextIndex[temInd] = args.PrevLogIndex + len(args.Entries) + 1 //len(rf.Log)
					rf.matchIndex[temInd] = args.PrevLogIndex + len(args.Entries)    //len(rf.Log) - 1
					rf.mu.Unlock()
					rf.updateCommitIndex()
					return
				}

				// fail for the out of date
				if reply.Term > rf.CurrentTerm {
					DPrintf("==AE REPLY== %v get bigger term", rf.me)
					rf.CurrentTerm = reply.Term
					rf.state = Follower
					rf.persist()
					rf.mu.Unlock()
					return
				}

				lastEntry, ok := rf.findLastLogInTerm(reply.ConflictTerm)
				if ok {
					rf.nextIndex[temInd] = lastEntry + 1
					DPrintf("==AE REPLY==%v -> %v  fail.nextIndex %v : %v -> %v", rf.me, temInd, temInd, args.PrevLogIndex+1, rf.nextIndex[temInd])
				} else {
					rf.nextIndex[temInd] = reply.ConflictIndex
					DPrintf("==AE REPLY==%v -> %v fail.nextIndex %v : %v -> %v", rf.me, temInd, temInd, args.PrevLogIndex+1, rf.nextIndex[temInd])
				}

				// prepare the args for next RPC
				copyPrevLogIndex = rf.nextIndex[temInd] - 1
				copyPrevLogTerm = rf.Log[copyPrevLogIndex].Term

				targetLog := rf.Log[rf.nextIndex[temInd]:]
				copyLog = make([]LogEntry, len(targetLog))
				copy(copyLog, targetLog)
				rf.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}
}

//
// background thread to send notify to the channel
//
func (rf *Raft) notifyCommit() {

	rf.lastApplied = 0
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.commitIndex > rf.lastApplied {
			for ind := rf.lastApplied + 1; ind <= rf.commitIndex; ind++ {
				DPrintf("==COMMIT== %v commits %v.", rf.me, ind)
				rf.applyChanel <- ApplyMsg{true, rf.Log[ind].Command, rf.Log[ind].Index,false,nil,0,0}
			}
			rf.lastApplied = rf.commitIndex
		}

		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// ================ Agreement ================


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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.Log)
	term := rf.CurrentTerm
	isLeader := !rf.killed() && (rf.state == Leader)

	// if not a Leader, return immediately
	if !isLeader {
		return index, term, isLeader
	}

	// start agreement on new command
	rf.Log = append(rf.Log, LogEntry{term, index, command})
	rf.persist()
	DPrintf("==START== %v start %v", rf.me, index)
	//go rf.getAgreement()

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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChanel = applyCh
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Log = make([]LogEntry, 0, 10)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.state = Follower
	rf.dead = 0
	rf.heartBeatCh = make(chan int, 10)
	rf.Log = append(rf.Log, LogEntry{0, 0, nil})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.notifyCommit()

	return rf
}
