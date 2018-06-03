package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "bytes"
import "log"
import "time"
import "sort"


const Debug = 1

//sort for []string
type StringList []string
func (s StringList) Len()int {return len(s)}
func (s StringList) Swap(i, j int) {s[i],s[j] = s[j],s[i]}
func (s StringList) Less(i, j int) bool {return s[i]<=s[j]}

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
	database map[int]map[string]string // configNum to database
	//store map[string]string   // store key value
	latestId map[int64]int    // id for each client
	commands map[int]Op       // operation
	commits map[int]chan bool
	mck *shardmaster.Clerk // shardmaster clerk
	configNum int // current configNum
	isKilled chan bool
	isKilledSig bool
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
	e.Encode(kv.database)
	e.Encode(kv.latestId)
	e.Encode(kv.configs)
	e.Encode(kv.configNum)
	
	
	data := w.Bytes()
	return data
	
}

func (kv *ShardKV) readSnapshot(data []byte){
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var database map[int]map[string]string
 
	var latestId map[int64]int 
	var configs shardmaster.Config
	var configNum int


	if d.Decode(&database) != nil ||
		d.Decode(&latestId) != nil ||
		d.Decode(&configs) != nil ||
		d.Decode(&configNum) != nil{
			DPrintf("Error!")
			return
	} else{
		kv.database = database
		kv.latestId = latestId
		kv.configs = configs
		kv.configNum = configNum
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
		//DPrintf("In SendSnapshot: %v", kv.store)
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

func (kv *ShardKV) CheckGroup() bool{
	if _,ok := kv.database[kv.configNum];ok == false{
		return false
	}
	for _,gid := range kv.configs.Shards{
		if gid == kv.gid{
			return true
		}
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.CheckGroup(){
		reply.Value = ""
		reply.WrongLeader = true
		reply.Err = ErrWrongGroup
		return
	}


	//kv.mu.Lock()
	//DPrintf("kv.database:%v", kv.database)
	//kv.mu.Unlock()

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
		value, ok := kv.database[kv.configNum][args.Key]
		kv.mu.Unlock()
    	if ok{
			DPrintf("kv.me:%v, In get database:%v", kv.me, kv.database)
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
	if !kv.CheckGroup(){
		//reply.Value = ""
		reply.WrongLeader = true
		reply.Err = ErrWrongGroup
		return 
	}

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

func (kv *ShardKV) GetLatestDB() map[string]string {
	return kv.database[kv.configNum]
}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	
	reply.WrongLeader = !isLeader
	gid := kv.gid
	configNum := kv.configNum
	getDB,ok := kv.database[args.ConfigNum]
	DPrintf("kv.me:%v Before GetShards, args.ConfigNum:%v, db:%v, kv.config:%v", kv.me, args.ConfigNum, kv.database, kv.configs)
	kv.mu.Unlock()

	DPrintf("kv.me:%v After GetShards, args.ConfigNum:%v", kv.me,getDB)

	if isLeader == false{
		reply.Err = ErrWrongLeader
		return
	}

	

	if gid != args.Gid || configNum < args.ConfigNum{
		reply.Err = ErrWrongGroup
		return
	}

	
	if ok == false{
		reply.Err = ErrWrongGroup
		return
	}else{
		DPrintf("in GetShards, database:%v", getDB)
		reply.Store = make(map[string]string)
		for k,v := range getDB{
			reply.Store[k] = v
		}
		reply.Err = OK
	}
}

func IsEqualGroups(s1 []string, s2 []string) bool{
	sort.Sort(StringList(s1))
	sort.Sort(StringList(s2))
	if len(s1) != len(s2){
		return false
	}
	for idx,s := range s1{
		if s != s2[idx]{
			return false
		}
	}
	return true
}

func (kv *ShardKV) Reconfigure(newConfig *shardmaster.Config) bool{
	kv.mu.Lock()
	curConfig := kv.configs
	kv.mu.Unlock()

	if _,ok := kv.database[newConfig.Num]; ok == true{
		return true
	}

	//isFindGid := false

	DPrintf("Check gid kv.gid:%v, kv.me:%v", kv.gid, kv.me)

	alldb := map[string]string{}

	for i:=0; i<shardmaster.NShards; i++{
		//check join same gid
		//isEqual := true

		//if newConfig.Shards[i] == kv.gid && curConfig.Shards[i] != -1{
		//	isFindGid = true
		//}

		//if curConfig.Shards[i] == kv.gid && newConfig.Shards[i] == kv.gid{
			
		//	s1 := curConfig.Groups[kv.gid]
		//	s2 := newConfig.Groups[kv.gid]
		//	isEqual = IsEqualGroups(s1,s2)
		//}

		if (newConfig.Shards[i] == kv.gid){
			for _,server := range curConfig.Groups[curConfig.Shards[i]]{
				server_end := kv.make_end(server)
				args := GetShardsArgs{Gid:curConfig.Shards[i], ConfigNum:newConfig.Num-1}
				reply := GetShardsReply{}
				reply.Store = make(map[string]string)
				DPrintf("kv.me:%v, before GetShards:%v", kv.me,  kv.database)
				server_end.Call("ShardKV.GetShards", &args, &reply)
				if reply.Err == OK{
					DPrintf("Here!!! find it!!!")
					//kv.mu.Lock()
					//kv.database[newConfig.Num] = reply.Store
					//kv.configNum = newConfig.Num
					//kv.configs = *newConfig

					for k,v := range reply.Store{
						alldb[k] = v
					}

					//kv.mu.Unlock()
					//return true
				}
			}
		}

		
		/*
		if (newConfig.Shards[i] == kv.gid && curConfig.Shards[i] != kv.gid && curConfig.Shards[i] != -1) || (isEqual == false){
			for _,server := range curConfig.Groups[curConfig.Shards[i]]{
				server_end := kv.make_end(server)
				args := GetShardsArgs{Gid:curConfig.Shards[i], ConfigNum:newConfig.Num-1}
				reply := GetShardsReply{}
				reply.Store = make(map[string]string)
				DPrintf("kv.me:%v, before GetShards:%v", kv.me,  kv.database)
				server_end.Call("ShardKV.GetShards", &args, &reply)
				if reply.Err == OK{
					DPrintf("Here!!! find it!!!")
					kv.mu.Lock()
					kv.database[newConfig.Num] = reply.Store
					kv.configNum = newConfig.Num
					kv.configs = *newConfig
					kv.mu.Unlock()
					return true
				}
			}
		}else if (newConfig.Shards[i] == kv.gid && curConfig.Shards[i] == kv.gid){
			kv.configNum = newConfig.Num
			kv.configs = *newConfig
			if _,ok := kv.database[newConfig.Num]; ok == false{
				kv.database[newConfig.Num] = make(map[string]string)
			}
			for k,v := range kv.database[newConfig.Num-1]{
				kv.database[newConfig.Num][k] = v
			}
			return true
		}
		*/

		
	}

	kv.mu.Lock()
	kv.database[newConfig.Num] = alldb
	kv.configNum = newConfig.Num
	kv.configs = *newConfig
	kv.mu.Unlock()


	return true
}

func (kv *ShardKV) CheckConfigure(){
	for true {
		if kv.isKilledSig{
			return
		}
		
		newConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		DPrintf("kv.me:%v, newConfig: %v, kv.configs.Num:%v, newConfig.Num:%v", kv.me, newConfig, kv.configs.Num, newConfig.Num)
		
		if kv.configs.Num < newConfig.Num{
			DPrintf("In CheckConfigure:%v", kv.configNum)
			kv.mu.Unlock()
			argConfig := kv.mck.Query(kv.configs.Num+1)
			DPrintf("kv.me:%v, argConfig:%v", kv.me, argConfig)
			
			kv.Reconfigure(&argConfig)
			//if ret == true{
			//	_,ok := kv.database[kv.configNum]
			//	if ok == false{
			//		kv.database[kv.configNum] = make(map[string]string)
			//	}
			//}
		}else{
			kv.mu.Unlock()
		}

		time.Sleep(time.Millisecond*100)
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
	close(kv.isKilled)
	//kv.isKilled <- true
	kv.isKilledSig = true
}

func (kv *ShardKV) Apply(){
	for true{
		select{
		case <- kv.isKilled:
			return
		case msg := <- kv.applyCh:
			if msg.CommandValid == true{
				DPrintf("Apply: %v, me:%v, configNum:%v", msg, kv.me, kv.configNum)
				command := msg.Command.(Op)
				kv.mu.Lock()
				DPrintf("kv.latestId[%v]: %v", command.ClientId, kv.latestId[command.ClientId])

				kv.commands[msg.CommandIndex] = command
				if kv.exist(command.Id, command.ClientId) == false{
					switch command.Operation{
					case "Put":
						//_,ok := kv.database[kv.configNum]
						//if ok == false{
						//	kv.database[kv.configNum] = make(map[string]string)In CheckConfigure:
						//}
						kv.database[kv.configNum][command.Key] = command.Value
						//kv.store[command.Key] = command.Value
					case "Append":
						if _,ok := kv.GetLatestDB()[command.Key];ok == false{
							kv.database[kv.configNum][command.Key] = command.Value
							//kv.store[command.Key] = command.Value
						}else{
							kv.database[kv.configNum][command.Key]  =  kv.database[kv.configNum][command.Key] + command.Value
							//kv.store[command.Key] = kv.store[command.Key] + command.Value
						}
					}
				}
				ch, ok := kv.commits[msg.CommandIndex]
				kv.mu.Unlock()
				_, isLeader := kv.rf.GetState()

				//commitMsgCh := kv.commits[msg.CommandIndex]

				DPrintf("After apply, kv.me:%v db:%v", kv.me, kv.database)

				
				if ok{
					select{
					case <- ch:
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

	kv.database = make(map[int]map[string]string)
	kv.database[0] = make(map[string]string)
	kv.latestId = make(map[int64]int)
	kv.commands = make(map[int]Op)
	kv.commits = make(map[int]chan bool)
	kv.mck = shardmaster.MakeClerk(masters)
	kv.configNum = 0
	kv.configs = shardmaster.Config{}
	for i:=0;i<shardmaster.NShards;i++{
		kv.configs.Shards[i] = -1
	}
	kv.isKilled = make(chan bool)
	kv.isKilledSig = false

	go kv.Apply()
	go kv.CheckConfigure()


	return kv
}
