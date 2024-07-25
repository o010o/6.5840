package raft

import (
	"errors"
	"log"
	"sync"
)

type logEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type raftLog struct {
	mu sync.RWMutex
	// apply may block because of net, so we have to try apply log entry until success. So just send log entry periodically.
	entries          []logEntry // pointer to content of log entry
	maxCommitedIndex int        // biggest index which can apply to state machine safely.
}

func min(lhs int, rhs int) int {
	if lhs < rhs {
		return lhs
	}
	return rhs
}

func max(lhs int, rhs int) int {
	if lhs > rhs {
		return lhs
	}
	return rhs
}

func (rf *raftLog) lock() {
	rf.mu.Lock()
}

func (rf *raftLog) unlock() {
	rf.mu.Unlock()
}

func (rf *raftLog) rLock() {
	rf.mu.RLock()
}

func (rf *raftLog) rUnLock() {
	rf.mu.RUnlock()
}

func (rf *raftLog) len() int {
	rf.rLock()
	defer rf.rUnLock()
	return len(rf.entries)
}

func (rl *raftLog) advanceCommited(commitedIndex int) bool {
	rl.lock()
	defer rl.unlock()

	if commitedIndex <= rl.maxCommitedIndex {
		return false
	}

	rl.maxCommitedIndex = commitedIndex

	if !rl.isIndexExist(rl.maxCommitedIndex) {
		log.Fatalf("advanceCommited: maxCommitedIndex not exist")
	}

	return true
}

func (rl *raftLog) getED(index int) (entryDescriptor, error) {
	rl.rLock()
	defer rl.rUnLock()

	if !rl.isIndexExist(index) {
		return entryDescriptor{-1, -1}, errors.New("index not exist")
	}

	return entryDescriptor{index, rl.entries[rl.slotId(index)].Term}, nil
}

func (rl *raftLog) construct() {
	rl.entries = make([]logEntry, 1)
	rl.entries[0] = logEntry{0, 0, nil}
	rl.maxCommitedIndex = 0
}

func (rl *raftLog) getMaxCommitedIndex() int {
	rl.rLock()
	defer rl.rUnLock()

	return rl.maxCommitedIndex
}

func (rl *raftLog) getMaxCommitedED() entryDescriptor {
	rl.rLock()
	defer rl.rUnLock()

	return entryDescriptor{rl.maxCommitedIndex, rl.entries[rl.slotId(rl.maxCommitedIndex)].Term}
}

func (rl *raftLog) getLatestED() entryDescriptor {
	rl.rLock()
	defer rl.rUnLock()

	return entryDescriptor{rl.entries[len(rl.entries)-1].Index, rl.entries[len(rl.entries)-1].Term}
}

func (rl *raftLog) isIndexExist(index int) bool {
	if len(rl.entries) == 0 {
		return false
	}

	if index < rl.entries[0].Index || index > rl.entries[len(rl.entries)-1].Index {
		return false
	}

	return true
}

func (rl *raftLog) isEntryExist(entry *entryDescriptor) bool {
	if !rl.isIndexExist(entry.Index) {
		return false
	}

	return rl.entries[rl.slotId(entry.Index)].Term == entry.Term
}

// return slot id of index
func (rl *raftLog) slotId(index int) int {
	if len(rl.entries) == 0 {
		return -1
	}

	// used before lock!
	return index - rl.entries[0].Index
}

func (rl *raftLog) getDiffSuffix(entries []logEntry) int {
	if len(entries) == 0 {
		log.Fatalf("getDiffSuffix: len of entries is 0")
	}

	i1 := rl.slotId(entries[0].Index)
	i2 := 0

	for ; i1 < len(rl.entries) && i2 < len(entries); i1, i2 = i1+1, i2+1 {
		if rl.entries[i1].Term != entries[i2].Term {
			break
		}
	}

	if i2 == len(entries) {
		return entries[i2-1].Index + 1
	}

	return entries[i2].Index
}

// return slice of entries, dont modify this slice
func (rl *raftLog) entriesAfter(index int, numsOpt ...int) (entryDescriptor, []logEntry, error) {
	rl.rLock()
	defer rl.rUnLock()

	if !rl.isIndexExist(index) {
		return entryDescriptor{-1, -1}, []logEntry{}, errors.New("invalid index")
	}

	if index == rl.entries[len(rl.entries)-1].Index {
		return entryDescriptor{index, rl.entries[rl.slotId(index)].Term}, []logEntry{}, nil
	}

	nums := -1
	if len(numsOpt) > 0 {
		nums = numsOpt[0]
	}

	if nums < 0 {
		// if term maintains
		// slice still can be used even there is new entry appends to log
		// if term end, and this server transform to follower, and the log was truncated.
		// slice returned can be accessed safely without segement, only the data we dont use would be changed
		entries := make([]logEntry, len(rl.entries)-rl.slotId(index+1))
		copy(entries, rl.entries[rl.slotId(index+1):])
		return entryDescriptor{index, rl.entries[rl.slotId(index)].Term}, entries, nil
	}

	// fetch [index + 1, index + 1 + nums)
	e := index + 1 + nums
	if e > rl.entries[len(rl.entries)-1].Index+1 {
		log.Fatalf("no enough entry to return")
	}

	entries := make([]logEntry, nums)
	copy(entries, rl.entries[rl.slotId(index+1):rl.slotId(e)])
	return entryDescriptor{index, rl.entries[rl.slotId(index)].Term}, entries, nil
}

func (rl *raftLog) checkConsistency(startIndex int) {
	if len(rl.entries) == 0 {
		return
	}

	if startIndex >= 0 {
		if rl.entries[0].Index != startIndex {
			log.Fatalf("assertIndex: start index inconsistency")
		}
	}

	for i := 1; i < len(rl.entries); i++ {
		if rl.entries[i].Index != rl.entries[i-1].Index+1 {
			log.Fatalf("assertIndex: Failed")
		}
		if rl.entries[i].Term < rl.entries[i-1].Term {
			log.Fatalf("assertIndex: Failed")
		}
	}
}
