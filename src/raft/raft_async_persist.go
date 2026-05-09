package raft

import (
	"bytes"
	"course/labgob"
)

func (rf *Raft) preparePersistData() ([]byte, []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persisted(e)
	raftstate := w.Bytes()

	snapshot := make([]byte, len(rf.log.snapshot))
	copy(snapshot, rf.log.snapshot)

	return raftstate, snapshot
}

func (rf *Raft) persistDirect(raftstate, snapshot []byte) {
	rf.persister.Save(raftstate, snapshot)
}
