package raft

import "time"

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
