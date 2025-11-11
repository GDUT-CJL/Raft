package server

import (
	"course/raft"
)

const (
	Set raft.OperationType = iota
	Delete
	Count

	HSet
	HDelete
	HCount

	RSet
	RDelete
	RCount

	BSet
	BDelete
	BCount

	ZSet
	ZDelete
	ZCount

	RCSet
	RCDelete
	RCCount
)

type OpReply struct {
	Value int
	Err   string
}

// 最后一次的提交记录信息
type LastOperationInfo struct {
	SeqId int64
	Reply *OpReply
}
