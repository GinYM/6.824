package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "bytes"
import "log"
import "time"


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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	configs shardmaster.Config
	store map[string]string   // store key value
	latestId map[int64]int    // id for each client
	commands map[int]Op       // operation
	commits map[int]chan bool
}

func (kv *ShardKV) getLastIncluded() int{
	smallest := -1
	for _,value := range kv.latestId{
		if smallest == -1{
			smallest = value
		}else{
			if value < smallest{
				smallest = value
			}
		}
	}
	return smallest 
}

func (kv *ShardKV) generateSHData() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.latestId)
	
	data := w.Bytes()
	return data
	
}

func (kv *ShardKV) readSnapshot(data []byte){
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var store map[string]string 
	var latestId map[int64]int 

	if d.Decode(&store) != nil ||
		d.Decode(&latestId) != nil{
			DPrintf("Error!")
			return
	} else{
		kv.store = store
		kv.latestId = latestId
	}
}


func (kv *ShardKV) SendSnapshot(index int){

	kv.mu.Lock()
	//defer kv.mu.Unlock()

	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate{
		_, isLeader := kv.rf.GetState()
		if isLeader == false{
			kv.mu.Unlock()
			return
		}
		data := kv.generateSHData()

		//DPrintf("SendSnapshot")
		DPrintf("In SendSnapshot: %v", kv.store)
		kv.mu.Unlock()
		kv.rf.SendInstallSnapshotAll(index, data)
	}else{
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) exist(commandId int, serverId int64) bool{
	if kv.latestId[serverId] >= commandId{
		return true
	}else{
		kv.latestId[serverId] = commandId
		return false
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Operation:"Get", Key:args.Key, Value:"", Id:args.CommandId, ClientId:args.ClientId}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	if isLeader == false{
    	reply.Err = ErrWrongLeader
    	reply.Value = ""
    	DPrintf("Not leader!")
    	return
    }

    kv.mu.Lock()

    ch,ok := kv.commits[index]
    if !ok{
    	kv.commits[index] = make(chan bool,1)
    	ch = kv.commits[index]
    }

    kv.mu.Unlock()

	select{
	case <-ch:  // uid := <-kv.commitGet:
		kv.mu.Lock()
		value, ok := kv.store[args.Key]
		kv.mu.Unlock()
    	if ok{
    		reply.Value = value
    		reply.Err = OK
    		DPrintf("Get value:%v", reply.Value)
    	}else{
    		DPrintf("Key error! No value for key: %v", args.Key)
    		reply.Value = ""
    		reply.Err = ErrNoKey
    	}
    	//kv.mu.Unlock()
    case <-time.After(time.Millisecond*1000):
    	DPrintf("Timeout!")
    	reply.Value = ""
    	reply.Err = ErrTimeOut
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Operation:args.Op, Key:args.Key, Value:args.Value, Id:(args.CommandId), ClientId:args.ClientId}
	index, _, isLeader := kv.rf.Start(op)



	reply.WrongLeader = !isLeader
	if isLeader == false {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	ch,ok := kv.commits[index]
    if !ok{
    	kv.commits[index] = make(chan bool,1)
    	ch = kv.commits[index]
    }

	//DPrintf("Waiting me: %v", kv.me)
	DPrintf("Put op:%v", op)

	kv.mu.Unlock()

	select{
	case <-ch:
		kv.mu.Lock()
		if kv.commands[index] == op{
			reply.Err = OK
		}else{
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
		kv.mu.Unlock()
		
    case <-time.After(time.Millisecond*1000):
    	DPrintf("PutAppendReply ErrTimeOut")
    	reply.Err = ErrTimeOut
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) Apply(){
	for true{
		select{
		//case <- kv.isKilled:
		//	break
		case msg := <- kv.applyCh:
			if msg.CommandValid == true{
				DPrintf("Apply: %v, me:%v", msg, kv.me)
				command := msg.Command.(Op)
				kv.mu.Lock()
				DPrintf("kv.latestId[%v]: %v", command.ClientId, kv.latestId[command.ClientId])

				kv.commands[msg.CommandIndex] = command
				if kv.exist(command.Id, command.ClientId) == false{
					switch command.Operation{
					case "Put":
						kv.store[command.Key] = command.Value
					case "Append":
						if _,ok := kv.store[command.Key];ok == false{
							kv.store[command.Key] = command.Value
						}else{
							kv.store[command.Key] = kv.store[command.Key] + command.Value
						}
					}
				}
				ch, ok := kv.commits[msg.CommandIndex]
				_, isLeader := kv.rf.GetState()

				commitMsgCh := kv.commits[msg.CommandIndex]

				kv.mu.Unlock()
				if ok{
					select{
					case <- commitMsgCh:
					default:
					}
					ch <- true
				}
				if isLeader{
					kv.SendSnapshot(msg.CommandIndex)
				}
			}else{
				kv.mu.Lock()
				DPrintf("Receive Snapshot")
				kv.readSnapshot(msg.SnapshotData)
				kv.mu.Unlock()
				//DPrintf("Command: %v", kv.store)
			}
			
			//kv.commits[msg.CommandIndex] <- true
		}
	}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.store = make(map[string]string)
	kv.latestId = make(map[int64]int)
	kv.commands = make(map[int]Op)
	kv.commits = make(map[int]chan bool)

	go kv.Apply()


	return kv
}
