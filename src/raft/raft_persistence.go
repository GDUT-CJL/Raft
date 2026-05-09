package raft

import (
	"bytes"
	"course/labgob"
	"time"
)

func (rf *Raft) persistLocked() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persisted(e)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.log.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	rf.electionTimout = 3 * MaxElectionTimeout
	rf.electionStart = time.Now()
	rf.restartTime = time.Now()

	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error:%v", err)
		return
	}
	rf.currentTerm = currentTerm

	var votedFor int
	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error:%v", err)
		return
	}
	rf.votedFor = votedFor

	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error:%v", err)
		return
	}

	rf.log.snapshot = rf.persister.ReadSnapshot()
	if rf.log.snapshot != nil && rf.log.snapLastIdx > 0 {
		rf.snapAppending = true
		rf.applyCond.Signal()

		if rf.log.snapLastIdx > rf.commitIndex {
			rf.commitIndex = rf.log.snapLastIdx
		}
		if rf.log.snapLastIdx > rf.lastApplied {
			rf.lastApplied = rf.log.snapLastIdx
		}
	}

	rf.resetElectionTimerLocked()
}
