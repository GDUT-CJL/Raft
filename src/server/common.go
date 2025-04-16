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
	Set OperationType = iota
	Get
)

func getOperationType(v string) OperationType {
	switch v {
	case "Set":
		return Set
	case "Get":
		return Get
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

