package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"math/rand"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string //"Put" or "Append"
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Operation:"Get", Key:args.Key, Value:""}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	if isLeader == false{
		reply.Err = ErrWrongLeader
		reply.Value = ""
		return
	}

	for true{
		msg := <-kv.applyCh
		if msg.CommandIndex != index || msg.Command != op{
			kv.applyCh <- msg
			time.Sleep(time.Duration(rand.Int()%50+50)*time.Millisecond)
		}else{
			value,ok := kv.store[args.Key] 
			if ok{
				reply.Value = value
				reply.Err = OK
			}else{
				reply.Value = ""
				reply.Err = ErrNoKey
			}
			break
		}
	}
	

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Operation:args.Op, Key:args.Key, Value:args.Value}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	if isLeader == false{
		reply.Err = ErrWrongLeader
		return
	}
	for true{
		msg := <-kv.applyCh
		if msg.CommandIndex != index || msg.Command != op{
			kv.applyCh <- msg
			time.Sleep(time.Duration(rand.Int()%50+50)*time.Millisecond)
		}else{
			kv.mu.Lock()
			if op.Operation == "Put"{
				kv.store[args.Key] = args.Value	
			}else{
				kv.store[args.Key] = kv.store[args.Key] + args.Value
			}
			reply.Err = OK
			kv.mu.Unlock()
			break
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.store = make(map[string]string)

	return kv
}
