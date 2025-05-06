package raft
import(
	"fmt"
	"course/labgob"
)
// 创建快照后，快照之前的日志条目不再在内存中直接可访问。节点可以从快照恢复状态，但快照后的日志tailLog能够继续访问
type RaftLog struct{
	snapLastIdx		int	// 快照的最后一个日志索引
	snapLastTerm	int	// 快照最后一个日志的任期号

	snapshot		[]byte	// 具体快照了哪些日志的切片
	tailLog			[]LogEntry	// 还没快照的日志
}

// 构造函数，创建一个新的快照日志
func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry)	*RaftLog{
	rl := &RaftLog{
		snapLastIdx	:	snapLastIdx,
		snapLastTerm:	snapLastTerm,
		snapshot:		snapshot,
	}

	rl.tailLog = make([]LogEntry,0,1+len(entries))
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)	
	return rl
}

// 将快照日志编码持久化
func (rl *RaftLog)readPersist(d *labgob.LabDecoder) error{
	var lastIndex int
	if err:=d.Decode(&lastIndex);err != nil{
		return fmt.Errorf("decode last include index failed")
	}
	rl.snapLastIdx = lastIndex

	var lastTerm int
	if err:=d.Decode(&lastTerm);err != nil{
		return fmt.Errorf("decode last include term failed")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
			return fmt.Errorf("decode tail log failed")
	}
	rl.tailLog = log

	return nil
}

// 将快照日志解码恢复
func (rl *RaftLog)persisted(e *labgob.LabEncoder){
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

// 应用层发送一个同步快照的请求给raft的leader层
func (rl *RaftLog)doSnapshot(index int,snapshot []byte){
	idx := rl.idx(index)

	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapLastIdx = index
	rl.snapshot = snapshot

	newLog:=make([]LogEntry,0,rl.size()-rl.snapLastIdx)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

// 其他peer从leader中同步快照
func (rl *RaftLog)installSnapshot(index,term int,snapshot []byte){
	rl.snapLastIdx = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	newLog:=make([]LogEntry,0,1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog

}

// 计算并返回 RaftLog 中的总日志条目数。
func (rl *RaftLog) size() int{
	return rl.snapLastIdx + len(rl.tailLog)
}

// 给定日志索引转换为 tailLog 中的索引。如果提供的索引不在有效范围内，则触发 panic。
func (rl *RaftLog) idx(logIndex int) int{
	if logIndex < rl.snapLastIdx || logIndex >= rl.size(){
		panic(fmt.Sprintf("%d is out of [%d, %d]", logIndex, rl.snapLastIdx+1, rl.size()-1))
	}
	return logIndex - rl.snapLastIdx
}

// 获取特定日志索引的日志条目。
// 依靠 idx 函数将外部索引转换为 tailLog 中的有效索引，并返回该索引对应的日志条目。
func (rl *RaftLog) at(logIndex int) LogEntry{
	return rl.tailLog[rl.idx(logIndex)]
}

// 找到term在Leader日志中的第一个条目的索引
func (rl *RaftLog)firstFor(term int) int{
	for i,entry := range rl.tailLog{
		if entry.Term == term{
			return i + rl.snapLastIdx
		}else if entry.Term > term{
			break
		}
	}
	return InvalidIndex
}

// more detailed
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIdx
	for i := 0; i < len(rl.tailLog); i++ {
			if rl.tailLog[i].Term != prevTerm {
					terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, i-1, prevTerm)
					prevTerm = rl.tailLog[i].Term
					prevStart = i
			}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, len(rl.tailLog)-1, prevTerm)
	return terms
}

// more simplified
func (rl *RaftLog) Str() string {
	lastIdx, lastTerm := rl.last()
	return fmt.Sprintf("[%d]T%d~[%d]T%d", rl.snapLastIdx, rl.snapLastTerm, lastIdx, lastTerm)
}

// 最后一条日志的索引和任期
func (rl *RaftLog) last() (idx, term int) {
	return rl.size() - 1, rl.tailLog[len(rl.tailLog)-1].Term
}

// 某个位置开始向后的日志
func (rl *RaftLog) tail(startIdx int)[]LogEntry{
	if startIdx >= rl.size(){
		return nil
	}

	return rl.tailLog[rl.idx(startIdx):]
}

// 增加一条日志
func (rl *RaftLog)append(e LogEntry){
	rl.tailLog = append(rl.tailLog,e)
}

// 从某个位置开始向后增加n条日志
func (rl *RaftLog)appendFrom(prevIndex int,entries []LogEntry){
	rl.tailLog = append(rl.tailLog[:rl.idx(prevIndex)+1],entries...)
}
// raft_log.go (需要补充日志操作方法)
// func (rl *RaftLog) slice(start, end int) []LogEntry {
//     // 实现日志切片逻辑
//     return rl.tailLog[start : end+1]
// }

// raft_log.go (需要补充日志操作方法)
func (rl *RaftLog) slice(start, end int) []LogEntry {
    if start < rl.snapLastIdx {
        start = rl.snapLastIdx + 1
    }
    if end >= len(rl.tailLog) {
        end = len(rl.tailLog) - 1
    }
    return rl.tailLog[start : end+1]
}



