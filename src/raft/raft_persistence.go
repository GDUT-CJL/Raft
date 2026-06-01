package raft

import (
	"bytes"
	"course/labgob"
	"time"
)

// preparePersistData 准备持久化数据 用于AppendEntriesRPC提前释放锁持久化
func (rf *Raft) preparePersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persisted(e)
	return w.Bytes()
}

// persistDirect 直接持久化数据（只写 WAL，不写快照文件）
// 用于 Start/RequestVote 等在锁外持久化的场景
func (rf *Raft) persistDirect(raftstate []byte) {
	rf.persister.SaveRaftState(raftstate)
}

func (rf *Raft) persistMeta(currentTerm, votedFor int) {
	rf.persister.SaveMeta(currentTerm, votedFor)
}

func (rf *Raft) persistLogReplace(prevIndex int, entries []LogEntry) {
	rf.persister.SaveLogReplace(prevIndex, entries)
}

func (rf *Raft) persistMetaAndLog(currentTerm, votedFor, prevIndex int, entries []LogEntry) {
	rf.persister.SaveMetaAndLog(currentTerm, votedFor, prevIndex, entries)
}

func (rf *Raft) persistLocked() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persisted(e)
	raftstate := w.Bytes()

	// 只写 WAL，不重复写快照文件（快照只在 Snapshot/InstallSnapshot 时写入）
	rf.persister.SaveRaftState(raftstate)
}

// prepareCompactData 准备快照压缩所需的 Raft 状态数据（在锁内调用，不拷贝快照）
// 快照数据由调用方直接传入，避免在锁内拷贝大块数据
func (rf *Raft) prepareCompactData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persisted(e)
	return w.Bytes()
}

// persistAndCompact 快照后持久化并压缩 WAL
// 快照后旧 WAL 中的历史记录不再需要，压缩为只包含最新状态的新 WAL
func (rf *Raft) persistAndCompact() {
	raftstate := rf.prepareCompactData()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.CompactWAL(raftstate, snapshot)
}

// persistAndCompactAsync 在锁外异步执行快照持久化和 WAL 压缩
// 调用方在锁内调用 prepareCompactData() 获取 raftstate，释放锁后调用此方法
// snapshot 由调用方在锁外传入（避免锁内拷贝大块数据）
func (rf *Raft) persistAndCompactAsync(raftstate, snapshot []byte) {
	rf.persister.CompactWAL(raftstate, snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 {
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
