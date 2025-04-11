package server


import (
	"fmt"
)


const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type OperationType uint8

const (
	OpSet OperationType = iota
	OpGet
)

func getOperationType(v string) OperationType {
	switch v {
	case "Set":
		return OpSet
	case "Get":
		return OpGet
	default:
		panic(fmt.Sprintf("unknown operation type %s", v))
	}
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OperationType
	ClientId int64
	SeqId    int64
}

