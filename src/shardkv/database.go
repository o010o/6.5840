package shardkv

import (
	"errors"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

type KVDatabase struct {
	data [shardctrler.NShards]map[string]string
}

func (kvd *KVDatabase) migrate(sId int, keys []string, values []string) {
	kvd.data[sId] = make(map[string]string)

	for i := 0; i < len(keys); i++ {
		kvd.data[sId][keys[i]] = values[i]
	}
}

func (kvd *KVDatabase) fetchKVs(sId int) ([]string, []string) {

	keys := make([]string, 0)
	values := make([]string, 0)

	for k, v := range kvd.data[sId] {
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values
}

func (kvd *KVDatabase) construct() {
	for i := 0; i < len(kvd.data); i++ {
		kvd.data[i] = make(map[string]string, 0)
	}

	labgob.Register(map[string]string{})
	labgob.Register([shardctrler.NShards]map[string]string{})
}

func (kvd *KVDatabase) get(key string) (string, error) {
	sId := key2shard(key)

	if v, ok := kvd.data[sId][key]; ok {
		return v, nil
	}
	return "", errors.New(ErrNoKey)
}

func (kvd *KVDatabase) append(key string, value string) {
	sId := key2shard(key)

	kvd.data[sId][key] = kvd.data[sId][key] + value
}

func (kvd *KVDatabase) put(key string, value string) {
	sId := key2shard(key)
	kvd.data[sId][key] = value
}

func (kvd *KVDatabase) delete(sId int) {
	kvd.data[sId] = map[string]string{}
}

func (kvd *KVDatabase) serialization(e *labgob.LabEncoder) {
	e.Encode(kvd.data)
}

func (kvd *KVDatabase) unSerialization(d *labgob.LabDecoder) {
	newData := [shardctrler.NShards]map[string]string{}
	for i := 0; i < len(newData); i++ {
		newData[i] = make(map[string]string)
	}

	if d.Decode(&newData) != nil {
		panic("unSerializate newData failed")
	}

	kvd.data = newData
}
