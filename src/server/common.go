package server

type OperationType uint8

const (
	Set OperationType = iota
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
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpType   OperationType
	// 每次请求都会生成ClientId和SeqId
	// ClientId和SeqId确定唯一的一次请求避免重复请求
	ClientId int64
	SeqId    int64
}

