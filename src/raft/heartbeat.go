package raft

import "time"

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
