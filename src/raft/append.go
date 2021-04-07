package raft

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
