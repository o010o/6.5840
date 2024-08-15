package raft

import (
	"log"
	"sync"
)

type stateMachine struct {
	mu      sync.RWMutex
	applied entryDescriptor
	applyCh chan ApplyMsg
}

func (m *stateMachine) getApplied() entryDescriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.applied
}

func (m *stateMachine) construct(ch chan ApplyMsg) {
	m.applyCh = ch
	m.applied.Index = 0
	m.applied.Term = 0
}

func (m *stateMachine) advanceApplied(applied *entryDescriptor) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.applied.getIndex() >= applied.getIndex() {
		return false
	}

	m.applied = *applied
	return true
}

func (m *stateMachine) reset(last entryDescriptor, data []byte) bool {
	if len(data) == 0 {
		return false
	}

	m.applyCh <- ApplyMsg{false, nil, 0, true, data, last.Term, last.Index}

	m.advanceApplied(&last)
	return true
}

func (m *stateMachine) applyLogEntries(entries []logEntry) {
	for _, entry := range entries {
		if debug {
			log.Printf("ready to apply %v", entry)
		}

		m.applyCh <- ApplyMsg{true, entry.Command, entry.Index, false, nil, 0, 0}
		if debug {
			log.Printf("apply %v done", entry)
		}

		m.advanceApplied(&entryDescriptor{entry.Index, entry.Term})
	}
}
