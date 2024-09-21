package shardctrler

type operationHistory struct {
	h map[int64]int64
}

func (oph *operationHistory) construct() {
	oph.h = make(map[int64]int64)
}

func (oph *operationHistory) find(op *Op) bool {
	// For Write, it is no need to store any result for lab4. But we still need to store if write has been exexute
	// For read, it does not matter to re-execute it. So we dont store result either.
	id := op.getId()
	requestId, ok := oph.h[id.ClientId]
	if !ok {
		return false
	}
	return id.RequestId == requestId
}

func (oph *operationHistory) insert(op *Op, r *execResult) {
	id := op.getId()
	oph.h[id.ClientId] = id.RequestId
}
