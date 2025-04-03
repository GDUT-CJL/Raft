package raft

import(
	"course/labgob"
	"bytes"
	"fmt"
)

func (rf *Raft) PersistStates() string{
	return fmt.Sprintf("T:%d,voted for :%d,log(0,%d]",rf.currentTerm,rf.votedFor,rf.log.size()) 
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
	// Your code here (PartC).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.persisted(e)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.log.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (PartC).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	
	var currentTerm 	int
	if err := d.Decode(&currentTerm);err != nil{
		LOG(rf.me,rf.currentTerm,DPersist,"Read currentTerm error:%v",err)
		return
	}
	rf.currentTerm = currentTerm

	var votedFor			int
	if err := d.Decode(&votedFor);err != nil{
		LOG(rf.me,rf.currentTerm,DPersist,"Read votedFor error:%v",err)
		return
	}
	rf.votedFor = votedFor

	// 日志的反序列化
	if err := rf.log.readPersist(d);err != nil{
		LOG(rf.me,rf.currentTerm,DPersist,"Read log error:%v",err)
		return
	}
	// 反序列化快照信息
	rf.log.snapshot = rf.persister.ReadSnapshot()
	// 如果快照的索引大于本地的提交日志的索引，则要进行更新
	if rf.log.snapLastIdx > rf.commitIndex{
		rf.commitIndex = rf.log.snapLastIdx
		rf.lastApplied = rf.log.snapLastIdx
	}
	LOG(rf.me, rf.currentTerm, DPersist, "Read Persist %v", rf.PersistStates())
}
