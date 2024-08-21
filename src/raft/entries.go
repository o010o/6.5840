package raft

import (
	"fmt"
	"log"
	"sync"
)

type logEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type snapshot struct {
	Data []byte
	Last entryDescriptor
}

func (s *snapshot) clone() snapshot {
	n := snapshot{}
	n.Data = make([]byte, len(s.Data))
	copy(n.Data, s.Data)
	n.Last = s.Last
	return n
}

type raftLog struct {
	mu               sync.RWMutex
	entries          []logEntry // pointer to content of log entry
	maxCommitedIndex int        // biggest index which can apply to state machine safely.
	s                snapshot   // snpashot
}

func (rl *raftLog) installSnapshot(last entryDescriptor, data []byte) bool {
	rl.lock()
	defer rl.unlock()

	if last.Index <= rl.maxCommitedIndex {
		// dont allow leader remove commited log entry
		return false
	}

	rl.resetSnapshot(last, data)

	if rl.isIndexInLog(last.Index) {
		rl.removeBeforeAnd(last.Index)
	} else {
		rl.clearLog()
	}

	return true
}

func (rl *raftLog) snapshot(index int, newSnapshot []byte) {
	rl.lock()
	defer rl.unlock()

	if index <= 0 || index > rl.getLastEntryIndex() {
		panic(fmt.Sprintf("snapshot: invalid index %v", index))
	}

	if index < rl.s.Last.Index {
		panic("Snapshot: invalid snapshot, last index of new snapshot smaller than old's, may cause entries lost")
	}

	if index > rl.maxCommitedIndex {
		panic("Snapshot: invalid snapshot, index bigger than max commited index")
	}

	last, err := rl.getED(index)
	if err != nil {
		panic(fmt.Sprintf("why get entry failed? index=%v, log={%v}", index, rl))
	}

	rl.resetSnapshot(last, newSnapshot)

	err = rl.removeBeforeAnd(index)
	if err != nil {
		log.Fatalf("snapshot: why remove failed, %v", err.Error())
	}
}

func (rl *raftLog) String() string {
	var str string

	str = "index=["
	if rl.s.Data != nil {
		str += fmt.Sprintf("~%v", rl.s.Last)
	}

	if len(rl.entries) > 0 {
		str += fmt.Sprintf("~%v", rl.entries[len(rl.entries)-1].Index)
	}

	str += "],"

	str += fmt.Sprintf("maxCommitedIndex=%v, snapshotLast=%v", rl.maxCommitedIndex, rl.s.Last)
	return str
}

func (rl *raftLog) resetSnapshot(last entryDescriptor, data []byte) {
	rl.s.Last = last
	rl.s.Data = data

	rl.advanceCommitedIndex(last.Index)
}

func (rl *raftLog) lock() {
	rl.mu.Lock()
}

func (rl *raftLog) unlock() {
	rl.mu.Unlock()
}

func (rl *raftLog) rLock() {
	rl.mu.RLock()
}

func (rl *raftLog) rUnLock() {
	rl.mu.RUnlock()
}

func (rl *raftLog) getSnapshotLast() entryDescriptor {
	rl.rLock()
	defer rl.rUnLock()

	return rl.s.Last
}

func (rl *raftLog) getLastEntryIndex() int {
	if len(rl.entries) != 0 {
		// the log may remain entry in snapshot for failure in our implmentent
		return max(rl.entries[len(rl.entries)-1].Index, rl.s.Last.Index)
	}

	return rl.s.Last.Index
}

func (rl *raftLog) getLastEntryIndexLock() int {
	rl.lock()
	defer rl.unlock()
	return rl.getLastEntryIndex()
}

func (rl *raftLog) advanceCommitedIndex(commitedIndex int) bool {
	if commitedIndex <= rl.maxCommitedIndex {
		return false
	}

	rl.maxCommitedIndex = commitedIndex

	if !rl.isIndexInSnapshot(rl.maxCommitedIndex) && !rl.isIndexInLog(rl.maxCommitedIndex) {
		panic("advanceCommitedIndex: maxCommitedIndex not exist")
	}

	return true
}

func (rl *raftLog) commit(commitedIndex int) bool {
	rl.lock()
	defer rl.unlock()

	return rl.advanceCommitedIndex(commitedIndex)
}

func (rl *raftLog) getEntry(index int) (pointerToEntry *logEntry, isInSnapshot bool, err error) {
	if index < 0 {
		log.Fatalf("getEntry: invalid index %v", index)
	}

	if rl.isIndexInSnapshot(index) {
		return nil, true, nil
	}

	if rl.isIndexInLog(index) {
		return &rl.entries[rl.slotId(index)], false, nil
	}

	// entry is not exist
	return nil, false, fmt.Errorf("getEntry: invalid index %v, maxIndex=%v", index, rl.getLastEntryIndex())
}

func (rl *raftLog) zeroEntryED() entryDescriptor {
	return entryDescriptor{0, 0}
}

func (rl *raftLog) getED(index int) (entryDescriptor, error) {
	if index == 0 {
		return rl.zeroEntryED(), nil
	}

	last := rl.s.Last
	if index == last.Index {
		return entryDescriptor{index, last.Term}, nil
	}

	e, _, err := rl.getEntry(index)
	if err != nil {
		return entryDescriptor{-1, -1}, fmt.Errorf("index %v is not exist, maxEntryIndex=%v", index, rl.getLastEntryIndex())
	}

	return entryDescriptor{index, e.Term}, nil
}

func (rl *raftLog) equalLock(term int, last int) (int, int) {
	rl.lock()
	defer rl.unlock()

	return rl.equal(term, last)
}

// lock outside
// get range of term in [rl.startAt, last]
// if index last not exist, return -1, -1
func (rl *raftLog) equal(term int, last int) (int, int) {
	if last <= 0 || last > rl.getLastEntryIndex() {
		// TODO: remove panic. New leader may remove log when executing snapshot, so last may bigger then the last
		panic(fmt.Sprintf("equal: invalid last %v", last))
	}

	if rl.isIndexInSnapshot(last) {
		return -1, -1
	}

	te := -1
	tb := -1
	// find entry whose has term before last
	for i := last; i > 0; i-- {
		e, inSnapshot, err := rl.getEntry(i)
		if err != nil || inSnapshot || e.Term < term {
			break
		}

		if e.Term == term {
			if te == -1 {
				te = i + 1
			}

			tb = i
		}
	}

	return tb, te
}

func (rl *raftLog) getEDLock(index int) (entryDescriptor, error) {
	rl.rLock()
	defer rl.rUnLock()

	return rl.getED(index)
}

func (rl *raftLog) construct() {
	rl.entries = make([]logEntry, 0)
	rl.s.Last = entryDescriptor{0, 0}
	rl.s.Data = nil
	rl.maxCommitedIndex = 0
}

func (rl *raftLog) getMaxCommitedIndex() int {
	rl.rLock()
	defer rl.rUnLock()

	return rl.maxCommitedIndex
}

func (rl *raftLog) appendNew(entries []logEntry) int {
	if len(entries) == 0 {
		return 0
	}

	if entries[0].Index < 1 || entries[0].Index > rl.getLastEntryIndex()+1 {
		return 0
	}

	if entries[0].Index == rl.getLastEntryIndex()+1 {
		rl.entries = append(rl.entries, entries...)
		return len(entries)
	}

	var firstDiff int
	for firstDiff = 0; firstDiff < len(entries); firstDiff++ {
		e, inSnapshot, err := rl.getEntry(entries[firstDiff].Index)
		if err != nil {
			// local log does not have entry(entries[firstDiff].Index)
			break
		}
		if !inSnapshot && e.Term != entries[firstDiff].Term {
			// find first conflict log entry
			break
		}
	}

	if firstDiff >= len(entries) {
		// no new entry
		return 0
	}

	if rl.isIndexInLog(entries[firstDiff].Index) {
		rl.removeAfterAnd(entries[firstDiff].Index)
	}

	rl.entries = append(rl.entries, entries[firstDiff:]...)

	return len(entries) - firstDiff
}

func (rl *raftLog) appendAt(pre entryDescriptor, entries []logEntry) (state AppendStateType, newAppendN int, conflict ConflictInfo) {
	// lock raft outside
	rl.lock()
	defer rl.unlock()

	if pre.Index < 0 {
		log.Fatalf("appendAt: pIndex should greater or equal than 0")
	}

	if pre.Index == 0 {
		return appendStateSucc, rl.appendNew(entries), ConflictInfo{-1, -1, rl.getLastEntryIndex() + 1}
	}

	if rl.isIndexInSnapshot(pre.Index) {
		return appendStateSucc, rl.appendNew(entries), ConflictInfo{-1, -1, rl.getLastEntryIndex() + 1}
	}

	if rl.isIndexInLog(pre.Index) {
		e, _, _ := rl.getEntry(pre.Index)
		if e.Term != pre.Term {
			// follower does not have pre, do not append entries
			minIndex, _ := rl.equal(e.Term, pre.Index)
			return appendStatePrevConflict, 0, ConflictInfo{e.Term, minIndex, rl.getLastEntryIndex() + 1}
		}
		return appendStateSucc, rl.appendNew(entries), ConflictInfo{-1, -1, rl.getLastEntryIndex() + 1}
	}

	return appendStatePrevNotExist, 0, ConflictInfo{-1, -1, rl.getLastEntryIndex() + 1}
}

func (rl *raftLog) append(term int, command interface{}) int {
	// lock raft outside
	rl.lock()
	defer rl.unlock()

	newIndex := rl.getLastEntryIndex() + 1

	rl.entries = append(rl.entries, logEntry{newIndex, term, command})

	return newIndex
}

func (rl *raftLog) getLatestED() entryDescriptor {
	rl.rLock()
	defer rl.rUnLock()

	ed, err := rl.getED(rl.getLastEntryIndex())
	if err != nil {
		log.Fatalf("getLatestED: %v", err.Error())
	}
	return ed
}

func (rl *raftLog) isIndexInSnapshot(index int) bool {
	return 0 < index && index <= rl.s.Last.Index
}

func (rl *raftLog) isIndexInLog(index int) bool {
	if len(rl.entries) == 0 {
		return false
	}

	return rl.entries[0].Index <= index && index <= rl.entries[len(rl.entries)-1].Index
}

// return slot id of index
func (rl *raftLog) slotId(index int) int {
	// FIXME: log entry in snapshot could exist

	if len(rl.entries) == 0 {
		panic("log is empty")
	}

	if rl.entries[0].Index <= index && index <= rl.entries[len(rl.entries)-1].Index {
		return index - rl.entries[0].Index
	}

	panic(fmt.Sprintf("slotId: invalid index, index=%v, maxEntryIndex=%v, snapshot last=%v", index, rl.getLastEntryIndex(), rl.s.Last))
}

func (rl *raftLog) clearLog() {
	rl.entries = make([]logEntry, 0)
}

func (rl *raftLog) removeBeforeAnd(index int) error {
	if index == 0 {
		panic("removeBeforeAnd: why index is zero")
	}

	if !rl.isIndexInLog(index) {
		return fmt.Errorf("removeBeforeAnd: entry before %v had been removed, raftLog={%v}", index, rl)
	}

	rl.entries = rl.entries[rl.slotId(index)+1:]
	return nil
}

func (rl *raftLog) removeAfterAnd(index int) {
	if index <= 0 || index > rl.getLastEntryIndex() {
		log.Fatalf("removeAfterAnd: invalid index, index=%v", index)
	}

	if rl.isIndexInSnapshot(index) {
		panic(fmt.Sprintf("removeAfterAnd: remove snapshot, index=%v", index))
	}

	rl.entries = rl.entries[:rl.slotId(index)]
}

// return last of snapshot, snapshot, previous descriptor, entries
func (rl *raftLog) entriesAfterAnd(index int, numsOpt ...int) (snapshot, entryDescriptor, []logEntry) {
	rl.rLock()
	defer rl.rUnLock()

	if index <= 0 || index > rl.getLastEntryIndex() {
		n := -1
		if len(numsOpt) > 0 {
			n = numsOpt[0]
		}
		log.Printf("log={%v}", rl)
		panic(fmt.Sprintf("entriesAfterAnd: invalid index, index=%v, lastEntryIndex=%v, nums=%v, snapshot=%v", index, rl.getLastEntryIndex(), n, rl.s.Last))
	}

	nums := rl.getLastEntryIndex() - index + 1
	if len(numsOpt) > 0 {
		if numsOpt[0] <= 0 {
			panic("entriesAfterAnd: nums should bigger than 0")
		}
		nums = min(numsOpt[0], nums)
	}

	s := snapshot{nil, entryDescriptor{}}
	preED := entryDescriptor{}
	entries := []logEntry{}

	if rl.isIndexInSnapshot(index) {
		s = rl.s.clone()
		nums = nums - (s.Last.Index - index + 1)
		index = s.Last.Index + 1
	}

	if nums > 0 {
		ed, err := rl.getED(index - 1)
		if err != nil {
			panic("entriesAfterAnd: get pre entry descriptor failed")
		}
		preED = ed

		entries = make([]logEntry, nums)
		sId := rl.slotId(index)
		copy(entries, rl.entries[sId:sId+nums])
	}

	return s, preED, entries
}

func (rl *raftLog) checkConsistency() {
	rl.lock()
	defer rl.unlock()

	if len(rl.entries) != 0 && rl.s.Last.Index != 0 {
		if rl.entries[0].Index-1 != rl.s.Last.Index {
			panic(fmt.Sprintf("index of first log entry:%v is not equal last index of snapshot:%v + 1 ", rl.entries[0].Index, rl.s.Last.Index))
		}
	}

	for i := 1; i < len(rl.entries); i++ {
		if rl.entries[i-1].Index+1 != rl.entries[i].Index {
			panic(fmt.Sprintf("entries[%v].Index: %v != entries[%v].Index:%v", i-1, rl.entries[i-1].Index, i, rl.entries[i].Index))
		}
	}
}
