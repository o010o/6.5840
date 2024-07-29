package raft

import (
	"log"
	"sync"
)

type stateMachine struct {
	mu      sync.RWMutex
	applyCh chan ApplyMsg
	applied entryDescriptor
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

func (m *stateMachine) applyLogEntry(e logEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e.Index != m.applied.Index+1 {
		log.Fatalf("invalid apply sequence")
	}

	m.applyCh <- ApplyMsg{true, e.Command, e.Index, false, []byte{}, 0, 0}
	m.applied.Term = e.Term
	m.applied.Index = e.Index
}

func (m *stateMachine) applyLogEntries(entries []logEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range entries {
		m.applyCh <- ApplyMsg{true, entry.Command, entry.Index, false, []byte{}, 0, 0}
		m.applied.Term = entry.Term
		m.applied.Index = entry.Index
	}

}
