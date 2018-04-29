package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	//"math/rand"
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
	Operation string //"Put" or "Append" or "Get"
	Key string
	Value string
	Id int
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string
	latestId map[int64]int
	commands []Op
	commitPutAppend chan bool
	commitGet chan bool

}

func (kv *KVServer) exist(commandId int, serverId int64) bool{
	if kv.latestId[serverId] >= commandId{
		return true
	}else{
		return false
	}
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("Get Key: %v", args.Key)
	op := Op{Operation:"Get", Key:args.Key, Value:"", Id:args.Id, ClientId:args.ClientId}

	if kv.exist(args.Id, args.ClientId){
		value, ok := kv.store[args.Key]
		if ok{
    		reply.Value = value
    		reply.Err = OK
    		DPrintf("Get value:%v", reply.Value)
    	}else{
    		reply.Value = ""
    		reply.Err = ErrNoKey
    	}
    	DPrintf("exists!! id:%v clientId:%d", args.Id, args.ClientId)
    	return
	}

	index, _, isLeader := kv.rf.Start(op)
	
	if isLeader == false{
    	reply.Err = ErrWrongLeader
    	reply.Value = ""
    	DPrintf("Not leader!")
    	return
    }

	select{
	case <-kv.commitGet:
		kv.mu.Lock()
		value, ok := kv.store[args.Key]
		defer kv.mu.Unlock()
		if index >= len(kv.commands) || (kv.commands[index] != op){
			reply.Value = ""
			reply.Err = ErrNoKey
			DPrintf("index: %v", index)
			DPrintf("len(commands): %v",len(kv.commands))
			return
		}
    	if ok{
    		reply.Value = value
    		reply.Err = OK
    		DPrintf("Get value:%v", reply.Value)
    	}else{
    		DPrintf("Key error! No value for key: %v", args.Key)
    		reply.Value = ""
    		reply.Err = ErrNoKey
    	}
    case <-time.After(time.Millisecond*1000):
    	DPrintf("Timeout!")
    	reply.Value = ""
    	reply.Err = ErrTimeOut
	}
	
}



func (kv *KVServer) update(id int, clientId int64){
	kv.latestId[clientId] = id
}

func (kv *KVServer) Apply(){
	for true{
		select{
		case msg := <- kv.applyCh:

			DPrintf("Apply: %v", msg)
			command := msg.Command.(Op)
			kv.update(command.Id, command.ClientId)
			kv.mu.Lock()
			_,isLeader := kv.rf.GetState()
			kv.mu.Unlock()

			DPrintf("Is Leader: %v, me: %v", isLeader, kv.me)

			kv.commands = append(kv.commands, command)

			if command.Operation == "Get"{
				if isLeader{
					kv.commitGet <- true
				}
			}else{
				DPrintf("latestId:%v msg:%v", kv.latestId[command.ClientId], msg)
					kv.mu.Lock()
					if command.Operation == "Put"{
						kv.store[command.Key] = command.Value
						DPrintf("Put command: %v", command)
					}else{
						DPrintf("Append command: %v", command)
						kv.store[command.Key] = kv.store[command.Key] + command.Value
					}
					kv.mu.Unlock()
					if isLeader{
						//DPrintf("PutAppend commit, me: %v",kv.me)
						kv.commitPutAppend <- true
					}
				
			}

			
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("Put Op: %v", args.Op)
	if kv.exist(args.Id, args.ClientId){
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
	
	op := Op{Operation:args.Op, Key:args.Key, Value:args.Value, Id:(args.Id), ClientId:args.ClientId}
	index, _, isLeader := kv.rf.Start(op)

	reply.WrongLeader = !isLeader
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	//DPrintf("Waiting me: %v", kv.me)

	select{
	case <-kv.commitPutAppend:
		if index >=len(kv.commands) || (kv.commands[index] != op){
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}else{
			reply.Err = OK
		}
		//reply.Err = OK
		//DPrintf("PutAppendReply Success: %v",reply.Err)
    case <-time.After(time.Millisecond*1000):
    	DPrintf("PutAppendReply ErrTimeOut")
    	reply.Err = ErrTimeOut
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
	kv.latestId = make(map[int64]int)
	kv.commands = []Op{}
	kv.commands = append(kv.commands, Op{})

	kv.commitPutAppend = make(chan bool)
	kv.commitGet = make(chan bool)

	go kv.Apply()

	return kv
}
