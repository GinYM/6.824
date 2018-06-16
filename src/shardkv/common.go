package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut = "ErrTimeOut"
	ErrMigration = "ErrMigration"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId int
	ClientId int64
	ConfigNum int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommandId int
	ClientId int64
	ConfigNum int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}


type GetShardsArgs struct {
	Gid int
	ConfigNum int
	Shard int
	//CommandId int
	//ClientId int64
}


type GetShardsReply struct {
	WrongLeader bool
	Err         Err
	Store map[string]string
	//ConfigNum int
}

type BroadcastArgs struct {
	Num int // desired config number
	Gid int

}

type BroadcastReply struct {
	WrongLeader bool
	Err         Err
	//Config      Config
}