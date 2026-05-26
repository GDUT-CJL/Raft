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
	snapshotFile = "snapshot.bin" // 快照仍使用独立文件
	walDir       = "wal"         // WAL 子目录
)

type Persister struct {
	mu      sync.Mutex
	peerDir string // 节点专属存储目录
	wal     *WAL   // 预写日志

	// 原子缓存的 WAL 大小，避免频繁 IO
	cachedWalSize int64

	// WAL 压缩期间暂存的写入请求
	pendingRecords [][]byte
	compacting     bool
}

func MakePersister(me int) *Persister {
	peerDir := filepath.Join(persistDir, fmt.Sprintf("peer%d", me))
	os.MkdirAll(peerDir, 0755)

	// 创建 WAL 目录
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

// RaftStateSize 返回 WAL 文件的总大小（使用缓存值，避免频繁 IO）
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

// Save 追加写入 Raft 状态到 WAL，快照写入独立文件
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	wal := ps.wal
	if ps.compacting {
		// WAL 正在压缩，暂存数据
		data := make([]byte, len(raftstate))
		copy(data, raftstate)
		ps.pendingRecords = append(ps.pendingRecords, data)
		ps.mu.Unlock()
		// 快照仍然写入
		if len(snapshot) > 0 {
			snapPath := filepath.Join(ps.peerDir, snapshotFile)
			if err := ps.saveSnapshotFile(snapPath, snapshot); err != nil {
				log.Printf("Failed to save snapshot: %v", err)
			}
		}
		return
	}
	ps.mu.Unlock()

	// 追加写入 Raft 状态到 WAL（带 fsync）
	if wal != nil {
		if err := wal.Append(WALRecordState, raftstate); err != nil {
			log.Printf("Failed to append WAL record: %v", err)
		}
	}

	// 快照写入独立文件（快照是大块数据，不适合追加到 WAL）
	if len(snapshot) > 0 {
		snapPath := filepath.Join(ps.peerDir, snapshotFile)
		if err := ps.saveSnapshotFile(snapPath, snapshot); err != nil {
			log.Printf("Failed to save snapshot: %v", err)
		}
	}
}

// SaveRaftState 只追加写入 Raft 状态到 WAL，不写快照文件
// 用于 persistLocked 等不需要更新快照的场景，避免不必要的快照 fsync
func (ps *Persister) SaveRaftState(raftstate []byte) {
	ps.mu.Lock()
	wal := ps.wal
	if ps.compacting {
		// WAL 正在压缩，暂存数据，压缩完成后补写
		data := make([]byte, len(raftstate))
		copy(data, raftstate)
		ps.pendingRecords = append(ps.pendingRecords, data)
		ps.mu.Unlock()
		return
	}
	ps.mu.Unlock()

	if wal != nil {
		if err := wal.Append(WALRecordState, raftstate); err != nil {
			log.Printf("Failed to append WAL record: %v", err)
		}
	}
}

// SaveBatch 批量保存（多条记录一次 fsync）
func (ps *Persister) SaveBatch(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	records := []WALRecord{
		{Type: WALRecordState, Data: raftstate},
	}

	if len(snapshot) > 0 {
		records = append(records, WALRecord{Type: WALRecordSnapshot, Data: snapshot})
	}

	if err := ps.wal.AppendBatch(records); err != nil {
		log.Printf("Failed to append WAL batch: %v", err)
	}

	// 快照仍然写入独立文件
	if len(snapshot) > 0 {
		snapPath := filepath.Join(ps.peerDir, snapshotFile)
		if err := ps.saveSnapshotFile(snapPath, snapshot); err != nil {
			log.Printf("Failed to save snapshot: %v", err)
		}
	}
}

// saveSnapshotFile 原子写入快照文件（带 fsync）
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

	// 显式 fsync 保证快照落盘
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()

	return os.Rename(tmpFile, path)
}

// ReadRaftState 从 WAL 中恢复最新的 Raft 状态
// 遍历所有 WAL 记录，取最后一条 WALRecordState 类型的记录
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	records, err := ps.wal.ReadAll()
	if err != nil {
		log.Printf("Failed to read WAL records: %v", err)
		return nil
	}

	// 取最后一条状态记录
	var lastState []byte
	for _, rec := range records {
		if rec.Type == WALRecordState {
			lastState = rec.Data
		}
	}

	return lastState
}

// ReadSnapshot 从独立文件读取快照
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

// CompactWAL 在快照后压缩 WAL，只保留最新状态
// 将当前状态作为新的 WAL 起点，删除旧 WAL 文件
func (ps *Persister) CompactWAL(raftstate []byte, snapshot []byte) {
	// 标记正在压缩，暂存期间的写入
	ps.mu.Lock()
	ps.compacting = true
	ps.mu.Unlock()

	// 在锁外做耗时 IO：关闭旧 WAL、删除文件、创建新 WAL
	ps.mu.Lock()
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

	// 写入当前状态作为新起点
	if err := newWAL.Append(WALRecordState, raftstate); err != nil {
		log.Printf("Failed to write initial WAL record after compaction: %v", err)
	}

	// 快照写入独立文件（不需要 ps.mu）
	if len(snapshot) > 0 {
		snapPath := filepath.Join(ps.peerDir, snapshotFile)
		if err := ps.saveSnapshotFile(snapPath, snapshot); err != nil {
			log.Printf("Failed to save snapshot during compaction: %v", err)
		}
	}

	// 在锁内替换为新 WAL，并补写暂存的数据
	ps.mu.Lock()
	ps.wal = newWAL
	ps.compacting = false
	pending := ps.pendingRecords
	ps.pendingRecords = nil
	ps.mu.Unlock()

	// 补写压缩期间暂存的数据（只保留最后一条，因为 Raft 状态是覆盖语义）
	if len(pending) > 0 {
		lastRecord := pending[len(pending)-1]
		if err := newWAL.Append(WALRecordState, lastRecord); err != nil {
			log.Printf("Failed to flush pending WAL record after compaction: %v", err)
		}
	}
}

// Close 关闭 Persister
func (ps *Persister) Close() {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.wal != nil {
		ps.wal.Close()
	}
}

// DecodeRaftState 从字节数组解码 Raft 状态（供 readPersist 使用）
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

	// 剩余部分是日志数据
	logData = r.Bytes()
	return
}
