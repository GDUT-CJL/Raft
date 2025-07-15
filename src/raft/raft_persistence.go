package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
	"time"
)

func (rf *Raft) PersistStates() string {
	return fmt.Sprintf("T:%d,voted for :%d,log(0,%d]", rf.currentTerm, rf.votedFor, rf.log.size())
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

// 在每次修改currentTerm、votedFor和log的时候调用persistLocked函数
func (rf *Raft) persistLocked() {
	// 创建一个新的字节缓冲区 w，作为编码数据的临时存储。
	w := new(bytes.Buffer)
	// 创建一个编码器 e，用于将数据编码到字节缓冲区 w
	e := labgob.NewEncoder(w)
	// 将当前任期号 rf.currentTerm 和被投票人ID rf.votedFor 编码存储。
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// 调用 rf.log.persisted(e) 方法，将日志的相关状态编码进去
	rf.log.persisted(e)
	// 获取编码后存储在缓冲区中的所有字节，保存到变量 raftstate
	raftstate := w.Bytes()
	// 调用 rf.persister.Save() 方法，将刚编码的持久状态 raftstate 和当前的快照（rf.log.snapshot）一起保存到持久化存储设备（如文件或数据库）
	rf.persister.Save(raftstate, rf.log.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

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

	// 日志的反序列化
	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error:%v", err)
		return
	}
	fmt.Printf("宕机前 Term:%d,votedFor:%d,role:%v\n", rf.currentTerm, rf.votedFor, rf.role)
	// 反序列化快照信息
	rf.log.snapshot = rf.persister.ReadSnapshot()
	// 如果快照的索引大于本地的提交日志的索引，则要进行更新
	if rf.log.snapshot != nil && rf.log.snapLastIdx > 0 {
		rf.snapAppending = true
		rf.applyCond.Signal() // 唤醒applyTicker

		if rf.log.snapLastIdx > rf.commitIndex {
			rf.commitIndex = rf.log.snapLastIdx
		}
		if rf.log.snapLastIdx > rf.lastApplied {
			rf.lastApplied = rf.log.snapLastIdx
		}
	}
	// if rf.votedFor == rf.me {
	// 	fmt.Println("Rejecting stale Leader status after restart")
	// 	rf.becomeFollowerLocked(rf.currentTerm + 1) // 主动降级
	// }
	//rf.electionTimout = MaxElectionTimeout // 强制让重新恢复的节点的超时时间增大
	rf.electionStart = time.Now()
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DPersist, "Read Persist %v", rf.PersistStates())
}
