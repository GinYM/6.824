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
	ConfigNum int
	TransferData map[string]string
	Config shardmaster.Config
	Gid int
	NewConfig shardmaster.Config
	
	//GetCacheData map[string]string
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
	//database map[int]map[string]string // configNum to database

	database map[string]string
	cacheData map[int]map[string]string	//configNum to store data from old server
	record map[int]int // gid to configNum(current configNum)
	
	//store map[string]string   // store key value
	latestId map[int64]int    // id for each client
	commands map[int]Op       // operation
	commits map[int]chan bool
	mck *shardmaster.Clerk // shardmaster clerk
	configNum int // current configNum
	isKilled chan bool
	isKilledSig bool
	isMigration bool // whether configuration change
	LockData map[int]bool // 
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
	e.Encode(kv.cacheData)
	e.Encode(kv.record)
	e.Encode(kv.commands)
	e.Encode(kv.isMigration)
	
	
	data := w.Bytes()
	return data
	
}

func (kv *ShardKV) readSnapshot(data []byte){
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var database map[string]string
	var latestId map[int64]int 
	var configs shardmaster.Config
	var configNum int
	var cachedData map[int]map[string]string
	var record map[int]int
	var commands map[int]Op
	var isMigration bool


	if d.Decode(&database) != nil ||
		d.Decode(&latestId) != nil ||
		d.Decode(&configs) != nil ||
		d.Decode(&configNum) != nil ||
		d.Decode(&cachedData) != nil ||
		d.Decode(&record) != nil ||
		d.Decode(&commands) != nil ||
		d.Decode(&isMigration) != nil {
			DPrintf("Error!")
			return
	} else{
		kv.database = database
		kv.latestId = latestId
		kv.configs = configs
		kv.configNum = configNum
		kv.cacheData = cachedData
		kv.record = record
		kv.commands = commands
		kv.isMigration = isMigration
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
		DPrintf("In SendSnapshot: %v", kv.database)
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

func (kv *ShardKV) CheckGroup(configNum int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("in checkgroup")
	DPrintf("kv.gid:%v, kv.me:%v, kv.configNum:%v, configNum:%v, kv.configs:%v", kv.gid, kv.me, kv.configNum, configNum, kv.configs)

	if kv.isMigration == true{
		return false
	}


	if kv.configNum != configNum{
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
	if !kv.CheckGroup(args.ConfigNum){
		//DPrintf("In ")
		reply.Value = ""
		reply.WrongLeader = false
		reply.Err = ErrWrongGroup
		return
	}


	//kv.mu.Lock()
	//DPrintf("kv.database:%v", kv.database)
	//kv.mu.Unlock()

	DPrintf("Request Get, gid:%v, key:%v", kv.gid, args.Key)

	op := Op{Operation:"Get", Key:args.Key, Value:"", Id:args.CommandId, ClientId:args.ClientId, ConfigNum:args.ConfigNum}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	if isLeader == false{
    	reply.Err = ErrWrongLeader
    	reply.Value = ""
    	DPrintf("Not leader! kv.gid:%v, kv.me:%v", kv.gid, kv.me)
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
	case ret := <-ch:  // uid := <-kv.commitGet:
		if ret == false{
			reply.Err = ErrWrongGroup
			DPrintf("gid:%v, kv.me:%v, ret is false", kv.gid, kv.me)
			return
		}
		kv.mu.Lock()
		value, ok1 := kv.database[args.Key]
		kvConfigNum := kv.configNum
		isMigration := kv.isMigration
		kv.mu.Unlock()
		if args.ConfigNum != kvConfigNum || isMigration == true{
			reply.Err = ErrWrongGroup
			DPrintf("gid:%v, kv.me:%v, configNum not equal", kv.gid, kv.me)
			return
		}
    	if ok1{
			DPrintf("gid:%v, kv.me:%v, In get database:%v", kv.gid, kv.me, kv.database)
    		reply.Value = value
    		reply.Err = OK
    		DPrintf("Get value:%v", reply.Value)
    	}else{
    		DPrintf("Key error! No value for key: %v", args.Key)
    		reply.Value = ""
    		reply.Err = ErrNoKey
    	}
    	//kv.mu.Unlock()
    case <-time.After(time.Millisecond*2000):
    	DPrintf("Timeout!, kv.me:%v, kv.gid:%v", kv.me, kv.gid)
    	reply.Value = ""
    	reply.Err = ErrTimeOut
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("In put append!")
	if !kv.CheckGroup(args.ConfigNum){
		//reply.Value = ""
		DPrintf("In put, wrong group!")
		reply.WrongLeader = true
		reply.Err = ErrWrongGroup
		return 
	}

	DPrintf("Check success!")

	DPrintf("putappendid:%v", args.CommandId)

	op := Op{Operation:args.Op, Key:args.Key, Value:args.Value, Id:(args.CommandId), ClientId:args.ClientId, ConfigNum:args.ConfigNum}
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
	case ret := <-ch:
		DPrintf("Receive apply, ret:%v", ret)
		if ret == false{
			reply.Err = ErrWrongGroup
			
			return
		}
		kv.mu.Lock()
		DPrintf("kv.commands[index]")
		DPrintf("kv.commands[index].id:%v, op.Id:%v, kv.isMigratioin:%v", kv.commands[index].Id, op.Id, kv.isMigration)
		if  kv.commands[index].Id == op.Id && kv.commands[index].ClientId == op.ClientId && kv.isMigration == false{
			reply.Err = OK
			
		}else{
			DPrintf("Here???")
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
		kv.mu.Unlock()
		
    case <-time.After(time.Millisecond*1000):
    	DPrintf("PutAppendReply ErrTimeOut")
    	reply.Err = ErrTimeOut
	}
}



func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply){
	kv.mu.Lock()
	//defer kv.mu.Unlock()

	//kv.isMigration = true
	
	//reply.WrongLeader = !isLeader
	gid := kv.gid
	configNum := kv.configNum
	kv.mu.Unlock()
	//configNum := kv.configNum

	if gid != args.Gid || configNum != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return 
	}

	//getDB,ok := kv.database[args.ConfigNum]
	DPrintf("kv.me:%v, gid:%v, Before GetShards, args.ConfigNum:%v, db:%v, kv.config:%v", kv.me, kv.gid, args.ConfigNum, kv.database, kv.configs)
	

	//DPrintf("kv.me:%v After GetShards, args.ConfigNum:%v", kv.me,getDB)

	//DPrintf("isLeader:%v, gid:%v, args.gid:%v, configNum:%v, args.configNum:%v", isLeader, kv.gid, args.Gid, configNum, args.ConfigNum)

	

	op := Op{Operation:"GetShards"}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false{
		return 
	}

	kv.mu.Lock()
	

	//DPrintf("In getShards:%v, args.Shard:%v", store, args.Shard)

	ch,ok := kv.commits[index]
    if !ok{
    	kv.commits[index] = make(chan bool,1)
    	ch = kv.commits[index]
	}
	kv.mu.Unlock()

	

	//if gid != args.Gid || configNum != args.ConfigNum{
	//kv.mu.Lock()

	

	//kv.mu.Unlock()
	reply.Store = make(map[string]string)
	select{
	case  <-ch:
		kv.mu.Lock()
		//kv.isMigration = true
		for k,v := range kv.database{
			shard := key2shard(k)
			if shard == args.Shard{
				reply.Store[k] = v
			}
		}

		kv.mu.Unlock()

		reply.Err = OK
		//for k,v := range store{
		//	reply.Store[k] = v
		//}
		DPrintf("Get data true, kv.gid:%v, kv.configNum:%v, kv.config:%v, reply.Store:%v", kv.gid, kv.configNum, kv.configs, reply.Store)
		return 
		
	case <-time.After(time.Millisecond*1000):
		reply.Err = ErrTimeOut
    	DPrintf("Get old server data ErrTimeOut")
    	return 
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

func (kv *ShardKV) FetchData(newConfig *shardmaster.Config) bool{
	kv.mu.Lock()
	curConfig := kv.configs
	gid := kv.gid
	_,ok := kv.cacheData[newConfig.Num]
	


	isFindGid := false
	for gid,_ := range newConfig.Groups{
		if gid == kv.gid{
			isFindGid = true
		}
	}
	kv.mu.Unlock()

	//if isFindGid == false{
	//	return true
	//}

	if ok{
		return true
	}

	//if _,ok := kv.database[newConfig.Num]; ok == true{
	//	return true
	//}

	//isFindGid := false

	DPrintf("Check gid kv.gid:%v, kv.me:%v", kv.gid, kv.me)

	//alldb := map[string]string{}
	transferData := map[string]string{}

	isFindFinal := true

	for i:=0; i<shardmaster.NShards && isFindGid; i++{
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


		if (curConfig.Shards[i] != -1 && newConfig.Shards[i] == gid && gid != curConfig.Shards[i]){
			DPrintf("in reconfigure, newConfig.Shards[i]:%v", newConfig.Shards)
			isFind := false
			for _,server := range curConfig.Groups[curConfig.Shards[i]]{
				server_end := kv.make_end(server)
				args := GetShardsArgs{}
				if newConfig.Num == 1{
					args = GetShardsArgs{Gid:curConfig.Shards[i], ConfigNum:-1, Shard:i}
				}else{
					args = GetShardsArgs{Gid:curConfig.Shards[i], ConfigNum:newConfig.Num-1, Shard:i}
				}
				
				reply := GetShardsReply{}
				reply.Store = make(map[string]string)
				DPrintf("gid:%v, kv.me:%v, before GetShards:%v, args:%v", kv.gid, kv.me,  kv.database, args)
				ok := server_end.Call("ShardKV.GetShards", &args, &reply)
				DPrintf("gid:%v, kv.me:%v, reply:%v", kv.gid, kv.me, reply)
				if ok == true && reply.Err == OK{
					isFind = true
					DPrintf("Here!!! find it!!!")
					//kv.mu.Lock()
					//kv.database[newConfig.Num] = reply.Store
					//kv.configNum = newConfig.Num
					//kv.configs = *newConfig
					//kv.mu.Lock()
					for k,v := range reply.Store{
						transferData[k] = v
						//kv.database[k] = v
					}
					break
					//kv.mu.Unlock()

					//kv.mu.Unlock()
					//return true
				}
			}
			if isFind == false{
				isFindFinal = false
				break
			}
		}		
	}

	if isFindFinal == false{
		return false
	}

	//kv.mu.Lock()
	//kv.database[newConfig.Num] = alldb
	//kv.configNum = newConfig.Num
	//kv.configs = *newConfig
	//kv.isMigration = false
	//kv.mu.Unlock()

	op := Op{TransferData:transferData, Config:*newConfig, Operation:"FetchData"}
	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false{
		return false
	}

	kv.mu.Lock()

	ch,ok := kv.commits[index]
    if !ok{
    	kv.commits[index] = make(chan bool,1)
    	ch = kv.commits[index]
	}

	kv.mu.Unlock()

	DPrintf("In reconfigure, kv.gid:%v, kv.me:%v, op:%v", kv.gid, kv.me, op)
	
	select{
	case  ret := <-ch:
		DPrintf("Transfer data true, kv.configNum:%v, kv.config:%v", kv.configNum, kv.configs)
		return ret
		
    case <-time.After(time.Millisecond*1000):
    	DPrintf("PutAppendReply ErrTimeOut")
    	return false
	}

	return true
}

func (kv *ShardKV) CheckRecord(newConfig *shardmaster.Config) bool{
	for gid,_ := range newConfig.Groups{
		// check here??
		if kv.record[gid] != newConfig.Num{
			DPrintf("Gid:%v, In checkrecord kv.record:%v, newConfig:%v",kv.gid, kv.record, newConfig)
			return false
		}
	}
	return true
}

func (kv *ShardKV) UpdateDatabase(newConfig *shardmaster.Config)bool{
	//kv.mu.Lock()
	//_,ok := kv.cacheData[newConfig.Num]
	//kv.mu.Unlock()
	//if ok == false{
	//	return false
	//}
	isFindGid := false
	allConfig := shardmaster.Config{}
	allConfig.Groups = map[int][]string{}
	allConfig.Num = newConfig.Num
	for k,v := range newConfig.Groups{
		allConfig.Groups[k] = v
		if k == kv.gid{
			isFindGid = true
		}
	}
	for k,v := range kv.configs.Groups{
		allConfig.Groups[k] = v
		if k == kv.gid{
			isFindGid = true
		}
	}

	if isFindGid == true{
		if kv.CheckRecord(&allConfig) == false{
			return false
		}
	}
	
	

	//if kv.CheckRecord(newConfig) == false{
	//	return false
	//}

	

	op := Op{ Operation:"UpdateDatabase", NewConfig:*newConfig}
	index, _, isLeader := kv.rf.Start(op)
	//reply.WrongLeader = !isLeader
	if isLeader == false{
		return false
	}

	kv.mu.Lock()

	ch,ok := kv.commits[index]
    if !ok{
    	kv.commits[index] = make(chan bool,1)
    	ch = kv.commits[index]
	}

	kv.mu.Unlock()

	DPrintf("In updatedatabase, kv.gid:%v, kv.me:%v", kv.gid, kv.me)
	
	select{
	case  <-ch:
		kv.mu.Lock()
		//kv.isMigration = false
		kv.mu.Unlock()
		//kv.record[args.Gid] = args.Num
		DPrintf("Broadcast data true, kv.record:%v, kv.configNum:%v, kv.config:%v", kv.record, kv.configNum, kv.configs)
		//reply.Err = OK
		return true
		
    case <-time.After(time.Millisecond*1000):
		DPrintf("PutAppendReply ErrTimeOut")
		//reply.Err = ErrTimeOut
    	return false
	}

	return true
	
	
}

func (kv *ShardKV) BroadcastAll(newConfig *shardmaster.Config) bool{
	isFindFinal := true
	isFindGid := false
	allConfigGroups := map[int][]string{}
	for k,v := range kv.configs.Groups{
		allConfigGroups[k] = v
	}
	for k,v  := range newConfig.Groups{
		allConfigGroups[k] = v
	}
	for theGid,_ := range allConfigGroups{
		if theGid == kv.gid{
			isFindGid = true
		}
	}
	if isFindGid == false{
		return true
	}

	// check whether fetch successfully
	kv.mu.Lock()
	_,ok := kv.cacheData[newConfig.Num]
	kv.mu.Unlock()
	if ok == false{
		return false
	}


	for theGid, servers := range allConfigGroups{
		isFind := false
		for _,server := range servers{
			server_end := kv.make_end(server)
			args := BroadcastArgs{Gid:kv.gid, Num:newConfig.Num}
			reply := BroadcastReply{}
			DPrintf("Send broadcase from %v to %v", kv.gid, theGid)
			//reply.Store = make(map[string]string)
			//DPrintf("gid:%v, kv.me:%v, before GetShards:%v, args:%v", kv.gid, kv.me,  kv.database, args)
			ok := server_end.Call("ShardKV.Broadcast", &args, &reply)
			if ok == false || reply.Err != OK{
				continue
			}else{
				isFind = true
				break
			}
		}
		if isFind == false{
			isFindFinal = false
			break
		}
	}
	return isFindFinal
}

func (kv *ShardKV) Broadcast(args *BroadcastArgs, reply *BroadcastReply){
	op := Op{ Operation:"Broadcast", Gid:args.Gid, ConfigNum:args.Num}
	index, _, isLeader := kv.rf.Start(op)
	reply.WrongLeader = !isLeader
	if isLeader == false{
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	ch,ok := kv.commits[index]
    if !ok{
    	kv.commits[index] = make(chan bool,1)
    	ch = kv.commits[index]
	}

	kv.mu.Unlock()

	DPrintf("In broadcast, kv.gid:%v, kv.me:%v, op:%v, args.Gid:%v", kv.gid, kv.me, op, args.Gid)
	
	select{
	case  <-ch:
		//kv.record[args.Gid] = args.Num
		DPrintf("Broadcast data true, kv.record:%v, kv.configNum:%v, kv.config:%v", kv.record, kv.configNum, kv.configs)
		reply.Err = OK
		return 
		
    case <-time.After(time.Millisecond*1000):
		DPrintf("PutAppendReply ErrTimeOut")
		reply.Err = ErrTimeOut
    	return 
	}

	return 
}

//func (kv *ShardKV) CheckRecord() bool {
//	for 
//}

func (kv *ShardKV) CheckConfigure(){
	ok := false
	for true {

		select {
		case <- kv.isKilled:
			return
		default:
			DPrintf("gid:%v, in kv.me:%v, db:%v, currentCOnfig:%v, kv.isMigration:%v", kv.gid, kv.me, kv.database, kv.configs, kv.isMigration)
			_, isLeader := kv.rf.GetState()
			newConfig := kv.mck.Query(-1)
			//kv.mu.Lock()
			//if kv.configs.Num < newConfig.Num{
			//	kv.isMigration = true
			//}else{
			//	kv.isMigration = false
			//}
			//kv.mu.Unlock()
			if isLeader == false{
				time.Sleep(time.Millisecond*100)
				continue
			} 
			//newConfig := kv.mck.Query(-1)
			DPrintf("gid:%v, in kv.me:%v,, configNum:%v,  newConfig:%v, currentCOnfig:%v, kv.isMigration:%v", kv.gid, kv.me, kv.configNum, newConfig, kv.configs, kv.isMigration)
			kv.mu.Lock()
			//DPrintf("kv.me:%v, newConfig: %v, kv.configs.Num:%v, newConfig.Num:%v", kv.me, newConfig, kv.configs.Num, newConfig.Num)
			
			if kv.configs.Num < newConfig.Num{
				DPrintf("In CheckConfigure:%v", kv.configNum)
				//kv.isMigration = true
				kv.mu.Unlock()
				argConfig := kv.mck.Query(kv.configs.Num+1)
				DPrintf("kv.me:%v, argConfig:%v", kv.me, argConfig)
				
				ok = kv.FetchData(&argConfig)
				
				if ok == false{
					time.Sleep(time.Millisecond*500)
					continue
				}

				DPrintf("After FetchData, kv.me:%v, kv.gid:%v, config:%v, kv.cached:%v, kv.record:%v, argConfig:%v", kv.me, kv.gid,  kv.configs, kv.cacheData, kv.record, argConfig)
				
				ok = kv.BroadcastAll(&argConfig)
				if ok == false{
					time.Sleep(time.Millisecond*500)
					continue
				}

				DPrintf("After broadcast")

				DPrintf("After broadcast, kv.me:%v, kv.gid:%v, config:%v, kv.cached:%v, kv.record:%v", kv.me, kv.gid,  kv.configs, kv.cacheData, kv.record)

				ok = kv.UpdateDatabase(&argConfig)
				if ok == false{
					time.Sleep(time.Millisecond*500)
					continue
				}

				DPrintf("After update database")

				//if ret == true{
				//	_,ok := kv.database[kv.configNum]
				//	if ok == false{
				//		kv.database[kv.configNum] = make(map[string]string)
				//	}
				//}
			}else{
				kv.mu.Unlock()
			}
			//else{
			//	kv.isMigration = false
			//	kv.mu.Unlock()
			//}

			time.Sleep(time.Millisecond*100)
		}

		
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

func (kv *ShardKV) HandleMsg(msg raft.ApplyMsg) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	command := msg.Command.(Op)
	switch command.Operation{
	case "Broadcast":
		kv.record[command.Gid] = command.ConfigNum
		return true
	case "Put":
		DPrintf("kv.isMigration:%v, kv.configNum:%v, command.ConfigNum:%v", kv.isMigration, kv.configNum, command.ConfigNum)
		if kv.isMigration == false && kv.configNum == command.ConfigNum && kv.exist(command.Id, command.ClientId) == false{
			kv.database[command.Key] = command.Value
			DPrintf("In Put kv.gid:%v, kv.me:%v, database:%v", kv.gid, kv.me, kv.database)
			return true
		}else{
			return false
		}
	case "UpdateDatabase":
		for k,v := range kv.cacheData[command.NewConfig.Num]{
			kv.database[k] = v
		}
		DPrintf("In update database gid:%v, me:%v, db:%v, cacheData:%v", kv.gid, kv.me, kv.database, kv.cacheData)
		kv.configs = command.NewConfig 
		kv.configNum = command.NewConfig.Num 
		delete(kv.cacheData, command.NewConfig.Num)
		kv.isMigration = false
		return true
	case "Append":
		DPrintf("In append kv.me:%v, kv.gid:%v kv.configNum:%v, command.ConfigNum:%v", kv.me, kv.gid, kv.configNum, command.ConfigNum)
		if kv.isMigration == false && kv.configNum == command.ConfigNum && kv.exist(command.Id, command.ClientId) == false{
			if _,ok1 := kv.database[command.Key];ok1 == false{
				kv.database[command.Key] = command.Value
				//kv.store[command.Key] = command.Value
			}else{
				kv.database[command.Key]  =  kv.database[command.Key] + command.Value
				//kv.store[command.Key] = kv.store[command.Key] + command.Value
			}
			DPrintf("kv.gid:%v, kv.me:%v, database:%v", kv.gid, kv.me, kv.database)
			return true
		}else{
			return false
		}
	case "Get":
		if  (kv.configNum == command.ConfigNum){
			return true
		}else{
			return false
		}
	case "FetchData":

		_,ok := kv.cacheData[command.Config.Num]
		if ok{
			return true
		}else{
			kv.cacheData[command.Config.Num] = make(map[string]string)
		}
		//if command.Config.Num == kv.configNum{
			//kv.configs = command.Config
			//kv.configNum = command.Config.Num
			for k,v := range command.TransferData{
				kv.cacheData[command.Config.Num][k] = v
			}
			DPrintf("kv.gid:%v, kv.me:%v, kv.configs:%v, kv.configNum:%v. kv.datavse:%v", kv.gid, kv.me, kv.configs, kv.configNum, kv.database)			
			return true
		//}else{
		//	return false
		//}
		
		

	case "GetShards":
		kv.isMigration = true
		return true
	}
	return true
	
}

func (kv *ShardKV) Apply(){
	for true{
		select{
		case <- kv.isKilled:
			return
		case msg := <- kv.applyCh:
			if msg.CommandValid == true{
				DPrintf("kv.me:%v, kv.gid:%v, Apply:%v", kv.me, kv.gid, msg)
				command := msg.Command.(Op)
				kv.commands[msg.CommandIndex] = command
				DPrintf("Apply msg:%v, kv.gid:%v, kv.me:%v", msg, kv.gid, kv.me)
				ret := kv.HandleMsg(msg)
				ch, ok := kv.commits[msg.CommandIndex]
				if ok{
					select{
					case <- ch:
					default:
					}
					ch <- ret
				}
				

				_, isLeader := kv.rf.GetState()
				if isLeader{
					kv.SendSnapshot(msg.CommandIndex)
				}

			}else{
				//DPrintf("Read snapshot")
				kv.mu.Lock()
				DPrintf("Receive Snapshot")
				kv.readSnapshot(msg.SnapshotData)
				DPrintf("In readsnapshot kv.database:%v, kv.gid:%v, kv.me:%v", kv.database, kv.gid, kv.me)
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
	DPrintf("Restart Server")
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

	kv.database = make(map[string]string)
	//kv.database[0] = make(map[string]string)
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
	kv.isMigration = false
	kv.cacheData = make(map[int]map[string]string)
	kv.record = make(map[int]int)

	go kv.Apply()
	go kv.CheckConfigure()


	return kv
}
