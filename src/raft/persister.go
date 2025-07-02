package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// 定义存储目录和文件名
const (
	persistDir    = "raft-persist"
	raftStateFile = "raftstate.bin" // 存储raftstate的文件路径
	snapshotFile  = "snapshot.bin"  //  存储snapshot的文件路径
)

type Persister struct {
	mu      sync.Mutex
	peerDir string // 节点专属存储目录
}

func MakePersister(me int) *Persister {
	// filepath.Join 函数用于拼接路径
	peerDir := filepath.Join(persistDir, fmt.Sprintf("peer%d", me))
	// 确保目录存在
	os.MkdirAll(peerDir, 0755)
	return &Persister{peerDir: peerDir}
}

// 辅助函数：原子写入文件
func atomicWriteFile(filename string, data []byte) error {
	tmpFile := filename + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, filename)
}

// 辅助函数：读取文件
func readFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := readFile(filepath.Join(ps.peerDir, raftStateFile))
	if err != nil {
		return nil // 文件不存在时返回空
	}
	return data
}

// func (ps *Persister) RaftStateSize() int {
// 	ps.mu.Lock()
// 	defer ps.mu.Unlock()
// 	return len(ps.raftstate)
// }

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 保存 Raft 状态
	raftPath := filepath.Join(ps.peerDir, raftStateFile)
	if err := atomicWriteFile(raftPath, raftstate); err != nil {
		log.Printf("Failed to save raft state: %v", err)
	}

	// 保存快照
	snapPath := filepath.Join(ps.peerDir, snapshotFile)
	if err := atomicWriteFile(snapPath, snapshot); err != nil {
		log.Printf("Failed to save snapshot: %v", err)
	}
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := readFile(filepath.Join(ps.peerDir, snapshotFile))
	if err != nil {
		return nil // 文件不存在时返回空
	}
	return data
}

// func (ps *Persister) SnapshotSize() int {
// 	ps.mu.Lock()
// 	defer ps.mu.Unlock()
// 	return len(ps.snapshot)
// }
