package raft

import (
	"bytes"
	"course/labgob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

const (
	persistDir   = "raft-persist"
	snapshotFile = "snapshot.bin"
	walDir       = "wal"
)

/*
核心职责
 1. WAL 文件的创建和管理
 2. 快照文件的原子写入（先写 `.tmp`，`fsync` 后 `rename`）
 3. 崩溃恢复时从 WAL 重放所有记录重建 Raft 状态
 4. Compaction 时重建 WAL，只保留最新状态
*/

type Persister struct {
	mu      sync.Mutex
	peerDir string
	wal     *WAL

	cachedWalSize  int64
	pendingRecords []WALRecord
	compacting     bool
}

type persistMetaRecord struct {
	CurrentTerm int
	VotedFor    int
}

type persistLogReplaceRecord struct {
	PrevIndex int
	Entries   []LogEntry
}

func MakePersister(me int) *Persister {
	peerDir := filepath.Join(persistDir, fmt.Sprintf("peer%d", me))
	os.MkdirAll(peerDir, 0755)

	walPath := filepath.Join(peerDir, walDir)
	wal, err := OpenWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to open WAL for peer%d: %v", me, err)
	}

	return &Persister{
		peerDir: peerDir,
		wal:     wal,
	}
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	wal := ps.wal
	ps.mu.Unlock()

	if wal == nil {
		return int(ps.cachedWalSize)
	}

	size := wal.Size()
	ps.cachedWalSize = size
	return int(size)
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	snapPath := filepath.Join(ps.peerDir, snapshotFile)
	return ps.getFileSize(snapPath)
}

func (ps *Persister) getFileSize(filename string) int {
	info, err := os.Stat(filename)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

func encodeGobRecord(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodePersistState(data []byte) (int, int, *RaftLog, error) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	if err := d.Decode(&currentTerm); err != nil {
		return 0, 0, nil, err
	}
	if err := d.Decode(&votedFor); err != nil {
		return 0, 0, nil, err
	}

	var snapLastIdx int
	var snapLastTerm int
	var tailLog []LogEntry
	if err := d.Decode(&snapLastIdx); err != nil {
		return 0, 0, nil, err
	}
	if err := d.Decode(&snapLastTerm); err != nil {
		return 0, 0, nil, err
	}
	if err := d.Decode(&tailLog); err != nil {
		return 0, 0, nil, err
	}

	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		tailLog:      tailLog,
	}
	if len(rl.tailLog) == 0 {
		rl.tailLog = []LogEntry{{Term: snapLastTerm}}
	}
	return currentTerm, votedFor, rl, nil
}

func buildStateBytes(currentTerm, votedFor int, rl *RaftLog) []byte {
	if rl == nil {
		rl = NewLog(InvalidIndex, InvalidTerm, nil, nil)
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(currentTerm)
	e.Encode(votedFor)
	rl.persisted(e)
	return w.Bytes()
}

func (ps *Persister) queueOrAppendRecords(records []WALRecord) {
	ps.mu.Lock()
	wal := ps.wal
	if ps.compacting {
		ps.pendingRecords = append(ps.pendingRecords, records...)
		ps.mu.Unlock()
		return
	}
	ps.mu.Unlock()

	if wal == nil {
		return
	}
	if err := wal.AppendBatch(records); err != nil {
		log.Printf("Failed to append WAL records: %v", err)
	}
}

// 写入一条 WALRecordState 记录（term + votedFor + log） 触发时机：Snapshot/InstallSnapshot 后的首次写入
func (ps *Persister) SaveRaftState(raftstate []byte) {
	ps.queueOrAppendRecords([]WALRecord{{Type: WALRecordState, Data: raftstate}})
}

// 写入一条 WALRecordMetadata 记录（term + votedFor） 触发时机：任期变更、投票
func (ps *Persister) SaveMeta(currentTerm, votedFor int) {
	data, err := encodeGobRecord(persistMetaRecord{
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
	})
	if err != nil {
		log.Printf("Failed to encode meta record: %v", err)
		return
	}
	ps.queueOrAppendRecords([]WALRecord{{Type: WALRecordMetadata, Data: data}})
}

// 写入一条 WALRecordLogReplace 记录（prevIndex + entries） 触发时机：每次日志追加
func (ps *Persister) SaveLogReplace(prevIndex int, entries []LogEntry) {
	entriesCopy := make([]LogEntry, len(entries))
	copy(entriesCopy, entries)

	data, err := encodeGobRecord(persistLogReplaceRecord{
		PrevIndex: prevIndex,
		Entries:   entriesCopy,
	})
	if err != nil {
		log.Printf("Failed to encode log replace record: %v", err)
		return
	}
	ps.queueOrAppendRecords([]WALRecord{{Type: WALRecordLogReplace, Data: data}})
}

// 写入两条记录（Meta + Log）
func (ps *Persister) SaveMetaAndLog(currentTerm, votedFor, prevIndex int, entries []LogEntry) {
	metaData, err := encodeGobRecord(persistMetaRecord{
		CurrentTerm: currentTerm,
		VotedFor:    votedFor,
	})
	if err != nil {
		log.Printf("Failed to encode meta record: %v", err)
		return
	}

	entriesCopy := make([]LogEntry, len(entries))
	copy(entriesCopy, entries)
	logData, err := encodeGobRecord(persistLogReplaceRecord{
		PrevIndex: prevIndex,
		Entries:   entriesCopy,
	})
	if err != nil {
		log.Printf("Failed to encode log replace record: %v", err)
		return
	}

	ps.queueOrAppendRecords([]WALRecord{
		{Type: WALRecordMetadata, Data: metaData},
		{Type: WALRecordLogReplace, Data: logData},
	})
}

// 写入 WAL + 可选的快照文件
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.SaveRaftState(raftstate)
	if len(snapshot) > 0 {
		snapPath := filepath.Join(ps.peerDir, snapshotFile)
		if err := ps.saveSnapshotFile(snapPath, snapshot); err != nil {
			log.Printf("Failed to save snapshot: %v", err)
		}
	}
}

func (ps *Persister) SaveBatch(raftstate []byte, snapshot []byte) {
	ps.Save(raftstate, snapshot)
}

func (ps *Persister) saveSnapshotFile(path string, data []byte) error {
	tmpFile := path + ".tmp"

	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()

	return os.Rename(tmpFile, path)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	records, err := ps.wal.ReadAll()
	if err != nil {
		log.Printf("Failed to read WAL records: %v", err)
		return nil
	}
	if len(records) == 0 {
		return nil
	}

	currentTerm := 0
	votedFor := -1
	rl := NewLog(InvalidIndex, InvalidTerm, nil, nil)

	for _, rec := range records {
		switch rec.Type {
		case WALRecordState: //完整 Raft 状态快照（term + votedFor + log） 触发时机：Snapshot/InstallSnapshot 后的首次写入
			term, vote, replayLog, err := decodePersistState(rec.Data)
			if err != nil {
				log.Printf("Failed to decode state record: %v", err)
				continue
			}
			currentTerm = term
			votedFor = vote
			rl = replayLog
		case WALRecordMetadata: // 元数据记录（term + votedFor） 触发时机：任期变更、投票
			var meta persistMetaRecord
			if err := labgob.NewDecoder(bytes.NewBuffer(rec.Data)).Decode(&meta); err != nil {
				log.Printf("Failed to decode meta record: %v", err)
				continue
			}
			currentTerm = meta.CurrentTerm
			votedFor = meta.VotedFor
		case WALRecordLogReplace: // 增量日志记录（prevIndex + entries） 触发时机：每次日志追加
			var replace persistLogReplaceRecord
			if err := labgob.NewDecoder(bytes.NewBuffer(rec.Data)).Decode(&replace); err != nil {
				log.Printf("Failed to decode log replace record: %v", err)
				continue
			}
			rl.appendFrom(replace.PrevIndex, replace.Entries)
		}
	}

	return buildStateBytes(currentTerm, votedFor, rl)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	snapPath := filepath.Join(ps.peerDir, snapshotFile)
	data, err := os.ReadFile(snapPath)
	if err != nil {
		return nil
	}
	return data
}

// 压缩 WAL，重建为新文件
func (ps *Persister) CompactWAL(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	ps.compacting = true
	oldWAL := ps.wal
	ps.mu.Unlock()

	oldWAL.Close()

	walPath := filepath.Join(ps.peerDir, walDir)
	os.RemoveAll(walPath)
	os.MkdirAll(walPath, 0755)

	newWAL, err := OpenWAL(walPath)
	if err != nil {
		log.Printf("Failed to recreate WAL after compaction: %v", err)
		ps.mu.Lock()
		ps.compacting = false
		ps.mu.Unlock()
		return
	}

	if err := newWAL.Append(WALRecordState, raftstate); err != nil {
		log.Printf("Failed to write initial WAL record after compaction: %v", err)
	}

	if len(snapshot) > 0 {
		snapPath := filepath.Join(ps.peerDir, snapshotFile)
		if err := ps.saveSnapshotFile(snapPath, snapshot); err != nil {
			log.Printf("Failed to save snapshot during compaction: %v", err)
		}
	}

	ps.mu.Lock()
	ps.wal = newWAL
	ps.compacting = false
	pending := append([]WALRecord(nil), ps.pendingRecords...)
	ps.pendingRecords = nil
	ps.mu.Unlock()

	if len(pending) > 0 {
		if err := newWAL.AppendBatch(pending); err != nil {
			log.Printf("Failed to flush pending WAL records after compaction: %v", err)
		}
	}
}

func (ps *Persister) Close() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.wal != nil {
		ps.wal.Close()
	}
}

func DecodeRaftState(data []byte) (currentTerm int, votedFor int, logData []byte, err error) {
	if len(data) < 1 {
		err = fmt.Errorf("empty raft state")
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if err = d.Decode(&currentTerm); err != nil {
		return
	}
	if err = d.Decode(&votedFor); err != nil {
		return
	}

	logData = r.Bytes()
	return
}
