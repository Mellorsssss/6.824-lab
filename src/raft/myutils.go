package raft

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
