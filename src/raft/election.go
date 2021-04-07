package raft

import (
	"math/rand"
	"time"
)


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