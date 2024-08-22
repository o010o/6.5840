package kvraft

import "6.5840/labgob"

type operationHistory struct {
	h map[int64]int64
}

func (oph *operationHistory) construct() {
	oph.h = make(map[int64]int64)
}

func (oph *operationHistory) find(op *Op) bool {
	// For Write, it is no need to store any result for lab4. But we still need to store if write has been exexute
	// For read, it does not matter to re-execute it. So we dont store result either.
	id, ok := oph.h[op.Id.ClientId]
	if !ok {
		return false
	}
	return op.Id.RequestId == id
}

func (oph *operationHistory) insert(op *Op, r *execOpResult) {
	oph.h[op.Id.ClientId] = op.Id.RequestId
}

func (oph *operationHistory) serialization(e *labgob.LabEncoder) {
	e.Encode(oph.h)
}

func (oph *operationHistory) unSerialization(d *labgob.LabDecoder) {
	n := make(map[int64]int64)
	if d.Decode(&n) != nil {
		panic("unSerialization history failed")
	}

	oph.h = n
}
