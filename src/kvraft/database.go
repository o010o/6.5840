package kvraft

import "errors"

type KVDatabase struct {
	// mu   sync.Mutex
	data map[string]string
}

func (kvd *KVDatabase) construct() {
	kvd.data = make(map[string]string, 0)
}

func (kvd *KVDatabase) get(key string) (string, error) {
	if v, ok := kvd.data[key]; ok {
		return v, nil
	}
	return "", errors.New("get: key is not exist")
}

func (kvd *KVDatabase) append(key string, value string) {
	kvd.data[key] = kvd.data[key] + value
}

func (kvd *KVDatabase) put(key string, value string) {
	kvd.data[key] = value
}

// func (kvd *KVDatabase) reset( /*newData*/ ) {

// }
