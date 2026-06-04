package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// WAL 记录类型
type WALRecordType uint8

const (
	WALRecordState    WALRecordType = iota + 1 // Raft 状态（currentTerm, votedFor, log）
	WALRecordSnapshot                          // 快照引用
)

const (
	WALRecordMetadata   WALRecordType = 3 // 元数据记录（包含 leaderId, term, lastIncludedIndex, lastIncludedTerm）
	WALRecordLogReplace WALRecordType = 4 // 日志替换记录（包含 log）
)

const (
	walHeaderSize  = 4 + 1 + 4 // CRC + Type + DataLen = 9 bytes
	walFileSuffix  = ".wal"
	walMaxFileSize = 64 * 1024 * 1024 // 64MB
)

// WALRecord 一条 WAL 记录
type WALRecord struct {
	Type WALRecordType
	Data []byte
}

// syncRequest Group Commit 的刷盘等待请求
type syncRequest struct {
	done chan struct{} // 刷盘完成后关闭此 channel
}

// WAL 预写日志（支持 Group Commit）
type WAL struct {
	mu sync.Mutex

	dir      string
	file     *os.File
	fileSize int64

	// Group Commit 机制
	syncInterval time.Duration  // 定期刷盘间隔
	syncQueue    []*syncRequest // 等待刷盘的请求队列
	syncTimer    *time.Timer    // 定期刷盘定时器
	syncWake     chan struct{}  // 通知刷盘协程有新数据
	syncDone     chan struct{}  // 关闭刷盘协程

	closed bool
}

// OpenWAL 打开或创建 WAL，找到最后一个 .wal 文件
func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir failed: %w", err)
	}

	w := &WAL{
		dir:          dir,
		syncInterval: 10 * time.Millisecond, // 10ms 批量刷盘一次
		syncQueue:    make([]*syncRequest, 0, 64),
		syncWake:     make(chan struct{}, 1),
		syncDone:     make(chan struct{}),
	}

	// 查找已有的 WAL 文件，打开最后一个继续追加
	walFiles := w.listWALFiles()
	if len(walFiles) > 0 {
		lastFile := walFiles[len(walFiles)-1]
		path := filepath.Join(dir, lastFile)
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("open existing WAL file failed: %w", err)
		}
		info, _ := f.Stat()
		w.file = f
		w.fileSize = info.Size()
		if _, err := f.Seek(w.fileSize, io.SeekStart); err != nil {
			f.Close()
			return nil, fmt.Errorf("seek WAL file failed: %w", err)
		}
	} else {
		if err := w.rotateFile(); err != nil {
			return nil, fmt.Errorf("create WAL file failed: %w", err)
		}
	}

	// 启动后台刷盘协程
	w.syncTimer = time.NewTimer(w.syncInterval)
	go w.syncLoop()

	return w, nil
}

// syncLoop 后台刷盘协程
// 核心思想：写入者只管写文件，不等待 fsync
// 由后台协程定期 fsync，然后通知所有等待的写入者
func (w *WAL) syncLoop() {
	for {
		select {
		case <-w.syncDone:
			// 关闭前做最后一次 fsync
			w.mu.Lock()
			if w.file != nil {
				w.file.Sync()
			}
			// 通知所有等待者
			for _, req := range w.syncQueue {
				close(req.done)
			}
			w.syncQueue = w.syncQueue[:0]
			w.mu.Unlock()
			return

		case <-w.syncWake:
			// 有新数据写入，立即刷盘
			w.doSync()

		case <-w.syncTimer.C:
			// 定时器触发，刷盘
			w.doSync()
			w.syncTimer.Reset(w.syncInterval)
		}
	}
}

// doSync 执行一次 fsync 并通知所有等待者
func (w *WAL) doSync() {
	w.mu.Lock()
	if len(w.syncQueue) == 0 {
		w.mu.Unlock()
		return
	}

	// 取出所有等待的请求
	pending := w.syncQueue
	w.syncQueue = make([]*syncRequest, 0, 64)

	// fsync
	if w.file != nil {
		w.file.Sync() // 这是阻塞调用！
	}

	w.mu.Unlock()

	// 通知所有等待者（在锁外执行，避免死锁）
	for _, req := range pending {
		close(req.done)
	}
}

// Append 追加一条记录到 WAL（Group Commit：写入文件后等待后台 fsync）
func (w *WAL) Append(recordType WALRecordType, data []byte) error {
	encoded, err := w.encodeRecord(recordType, data)
	if err != nil {
		return err
	}

	// 注册刷盘等待请求
	req := &syncRequest{done: make(chan struct{})}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("WAL is closed")
	}

	// 写入文件（不 fsync，数据进入内核缓冲区）
	if _, err := w.file.Write(encoded); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("write WAL record failed: %w", err)
	}
	w.fileSize += int64(len(encoded))

	// 加入等待队列
	w.syncQueue = append(w.syncQueue, req)

	// 检查是否需要滚动文件
	if w.fileSize >= walMaxFileSize {
		if err := w.rotateFile(); err != nil {
			w.mu.Unlock()
			return fmt.Errorf("rotate WAL file failed: %w", err)
		}
	}

	w.mu.Unlock()

	// 通知刷盘协程有新数据
	select {
	case w.syncWake <- struct{}{}:
	default:
	}

	// 等待 fsync 完成（保证 Raft 安全性）
	<-req.done

	return nil
}

// AppendBatch 批量追加记录（多条记录写入后等待一次 fsync）
func (w *WAL) AppendBatch(records []WALRecord) error {
	// 编码所有记录
	encodedList := make([][]byte, 0, len(records))
	totalLen := 0
	for _, rec := range records {
		encoded, err := w.encodeRecord(rec.Type, rec.Data)
		if err != nil {
			return err
		}
		encodedList = append(encodedList, encoded)
		totalLen += len(encoded)
	}

	// 注册刷盘等待请求
	req := &syncRequest{done: make(chan struct{})}

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("WAL is closed")
	}

	// 批量写入文件
	buf := make([]byte, 0, totalLen)
	for _, encoded := range encodedList {
		buf = append(buf, encoded...)
	}

	if _, err := w.file.Write(buf); err != nil {
		w.mu.Unlock()
		return fmt.Errorf("write WAL batch failed: %w", err)
	}
	w.fileSize += int64(len(buf))

	// 加入等待队列（只注册一个请求）
	w.syncQueue = append(w.syncQueue, req)

	// 检查是否需要滚动文件
	if w.fileSize >= walMaxFileSize {
		if err := w.rotateFile(); err != nil {
			w.mu.Unlock()
			return fmt.Errorf("rotate WAL file failed: %w", err)
		}
	}

	w.mu.Unlock()

	// 通知刷盘协程
	select {
	case w.syncWake <- struct{}{}:
	default:
	}

	// 等待 fsync 完成（保证 Raft 安全性）
	<-req.done // ← 这里会阻塞，直到 fsync 完成

	return nil
}

// SyncNow 立即执行 fsync（用于关键路径，如投票、任期变更）
func (w *WAL) SyncNow() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

// encodeRecord 编码一条 WAL 记录
/*
编码格式：
	┌─────────┬──────┬──────────┬─────────────────┐
	│   CRC   │ Type │ DataLen  │      Data       │
	│  4 bytes│1 byte│ 4 bytes  │  DataLen bytes  │
	└─────────┴──────┴──────────┴─────────────────┘
		↓        ↓        ↓              ↓
	校验和   记录类型  数据长度      实际数据
*/
func (w *WAL) encodeRecord(recordType WALRecordType, data []byte) ([]byte, error) {
	totalLen := walHeaderSize + len(data) // 9 + dataLen
	buf := make([]byte, totalLen)

	buf[4] = byte(recordType)                                 // 第5字节：类型
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(data))) // 第6-9字节：长度
	copy(buf[walHeaderSize:], data)                           // 数据

	crc := crc32.ChecksumIEEE(buf[4:])          // CRC 覆盖类型+长度+数据
	binary.LittleEndian.PutUint32(buf[0:], crc) // 第1-4字节：CRC

	return buf, nil
}

// ReadAll 读取所有 WAL 文件中的记录
func (w *WAL) ReadAll() ([]WALRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	walFiles := w.listWALFiles()
	var allRecords []WALRecord

	for _, wf := range walFiles {
		path := filepath.Join(w.dir, wf)
		records, err := w.readFile(path)
		if err != nil {
			log.Printf("WAL: read file %s failed: %v", wf, err)
			continue
		}
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}

// readFile 读取单个 WAL 文件中的所有记录
func (w *WAL) readFile(path string) ([]WALRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var records []WALRecord
	offset := 0

	for offset+walHeaderSize <= len(data) {
		expectedCRC := binary.LittleEndian.Uint32(data[offset:])
		actualCRC := crc32.ChecksumIEEE(data[offset+4:])
		if actualCRC != expectedCRC {
			log.Printf("WAL: CRC mismatch at offset %d in %s, truncating", offset, path)
			break
		}

		recordType := WALRecordType(data[offset+4])
		dataLen := binary.LittleEndian.Uint32(data[offset+5:])

		if offset+walHeaderSize+int(dataLen) > len(data) {
			log.Printf("WAL: incomplete record at offset %d in %s, truncating", offset, path)
			break
		}

		recordData := make([]byte, dataLen)
		copy(recordData, data[offset+walHeaderSize:offset+walHeaderSize+int(dataLen)])

		records = append(records, WALRecord{
			Type: recordType,
			Data: recordData,
		})

		offset += walHeaderSize + int(dataLen)
	}

	return records, nil
}

// rotateFile 滚动到新的 WAL 文件
func (w *WAL) rotateFile() error {
	if w.file != nil {
		w.file.Sync()
		w.file.Close()
	}

	walFiles := w.listWALFiles()
	seq := 0
	if len(walFiles) > 0 {
		last := walFiles[len(walFiles)-1]
		fmt.Sscanf(last, "%d"+walFileSuffix, &seq)
		seq++
	}

	path := filepath.Join(w.dir, fmt.Sprintf("%08d%s", seq, walFileSuffix))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	w.file = f
	w.fileSize = 0
	return nil
}

// listWALFiles 列出所有 WAL 文件，按序号排序
func (w *WAL) listWALFiles() []string {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), walFileSuffix) {
			files = append(files, entry.Name())
		}
	}

	sort.Strings(files)
	return files
}

// Close 关闭 WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	// 停止刷盘协程
	close(w.syncDone)

	if w.syncTimer != nil {
		w.syncTimer.Stop()
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// Size 返回所有 WAL 文件的总大小
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	walFiles := w.listWALFiles()
	var total int64
	for _, wf := range walFiles {
		path := filepath.Join(w.dir, wf)
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}
