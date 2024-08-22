package kvraft

import (
	"errors"
	"sync"

	"6.5840/labgob"
)

type KVDatabase struct {
	mu   sync.Mutex
	last int
	data map[string]string
}

func (kvd *KVDatabase) advanceLast(newIndex int) {
	if newIndex <= kvd.last {
		panic("advanceLast: new index smaller than old")
	}
	kvd.last = newIndex
}

func (kvd *KVDatabase) construct() {
	kvd.mu.Lock()
	defer kvd.mu.Unlock()

	kvd.data = make(map[string]string, 0)

	labgob.Register(map[string]string{})
}

func (kvd *KVDatabase) get(index int, key string) (string, error) {
	kvd.mu.Lock()
	defer kvd.mu.Unlock()

	kvd.advanceLast(index)

	if v, ok := kvd.data[key]; ok {
		return v, nil
	}
	return "", errors.New("get: key is not exist")
}

func (kvd *KVDatabase) append(index int, key string, value string) {
	kvd.mu.Lock()
	defer kvd.mu.Unlock()

	kvd.advanceLast(index)

	kvd.data[key] = kvd.data[key] + value
}

func (kvd *KVDatabase) put(index int, key string, value string) {
	kvd.mu.Lock()
	defer kvd.mu.Unlock()

	kvd.advanceLast(index)

	kvd.data[key] = value
}

func (kvd *KVDatabase) serialization(e *labgob.LabEncoder) int {
	kvd.mu.Lock()
	defer kvd.mu.Unlock()

	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	e.Encode(kvd.data)

	// TODO:
	return kvd.last
}

func (kvd *KVDatabase) unSerialization(lastIndex int, d *labgob.LabDecoder) {
	kvd.mu.Lock()
	defer kvd.mu.Unlock()

	newData := make(map[string]string)
	if d.Decode(&newData) != nil {
		panic("unSerializate newData failed")
	}

	kvd.data = newData
	kvd.last = lastIndex
}
