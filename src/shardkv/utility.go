package shardkv

import (
	"fmt"
)

type notifyRecord struct {
	term   int
	indexs []int
	resChs []chan *execResult
}

func generateNotifyRecord() *notifyRecord {
	return &notifyRecord{-1, make([]int, 0), make([]chan *execResult, 0)}
}

func (nr *notifyRecord) push(term int, index int, ch chan *execResult) {
	if term < 0 || index < 0 || ch == nil {
		panic("invalid argument")
	}

	n := nr.tryNotifyOlder(term)
	if n > 0 {
		nr.clear()
	}

	if !(nr.term == -1 || nr.term == term) {
		panic("push")
	}

	nr.term = term
	nr.indexs = append(nr.indexs, index)
	nr.resChs = append(nr.resChs, ch)
}

func (nr *notifyRecord) front() (int, int, chan *execResult) {
	if nr.empty() {
		panic("front")
	}
	return nr.term, nr.indexs[0], nr.resChs[0]
}

func (nr *notifyRecord) popFront() {
	if nr.empty() {
		panic("popFront")
	}

	nr.indexs = nr.indexs[1:]
	nr.resChs = nr.resChs[1:]
	if nr.empty() {
		nr.term = -1
	}
}

func (nr *notifyRecord) empty() bool {
	return len(nr.indexs) == 0
}

func (nr *notifyRecord) clear() {
	*nr = *generateNotifyRecord()
}

func (nr *notifyRecord) getTerm() int {
	return nr.term
}

func (nr *notifyRecord) size() int {
	return len(nr.indexs)
}

func (nr *notifyRecord) tryNotifyOlder(term int) int {
	if nr.empty() {
		return 0
	}

	if term == nr.getTerm() {
		return 0
	}

	n := nr.size()

	for !nr.empty() {
		_, _, c := nr.front()
		c <- &execResult{ErrWrongLeader, nil}
		nr.popFront()
	}
	return n
}

func (nr *notifyRecord) tryNotify(term int, index int, res *execResult) int {
	n := nr.tryNotifyOlder(term)
	if nr.empty() {
		return 0
	}

	_, i, c := nr.front()
	if i == index {
		nr.popFront()
		c <- res
		return n + 1
	} else if i < index {
		panic(fmt.Sprintf("notify missed, notify %v, but index of front is %v", index, i))
	}

	return n
}

func (kv *ShardKV) tryNotify(nr *notifyRecord, index int, res *execResult) int {
	if nr == nil {
		panic("tryNotify: nr is nil")
	}

	if nr.empty() {
		return 0
	}

	// TODO: The term here may not corespond to index.
	// So, we may successfully execute operation, but we report that it did not execute successfully.
	// It may cause the client to re-execute the operaion

	term, _ := kv.rf.GetState()
	if index < 0 {
		return nr.tryNotifyOlder(term)
	}

	return nr.tryNotify(term, index, res)
}

type indexRecord struct {
	// one index coresponding to state machine, do not permit same index have two different state machine.
	last int
}

func (ir *indexRecord) advanceLast(newIndex int) {
	if newIndex <= ir.last {
		panic(fmt.Sprintf("advanceLast: new index is not bigger than old index, new=%v, old=%v", newIndex, ir.last))
	}
	ir.last = newIndex
}

func (ir *indexRecord) getLast() int {
	return ir.last
}

func (ir *indexRecord) setLast(last int) {
	ir.last = last
}
