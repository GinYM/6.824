package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "log"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Pair struct{
	key int
	value int
}

type PairList []Pair

func(p PairList) Len() int {return len(p)}
func(p PairList) Swap(i, j int) {p[i],p[j] = p[j],p[i]}
func(p PairList) Less(i, j int) bool {return p[i].value < p[j].value}


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	latestId map[int64]int    // id for each client
	commands map[int]Op       // operation
	commits map[int]chan bool
	configIdx int
}


type Op struct {
	// Your data here.
	Operation string
	Servers map[int][]string
	GIDs []int
	Shard int
	GID   int
	Num int
	CommandId int
	ClientId int64
}

func (sm *ShardMaster) exist(commandId int, serverId int64) bool{
	if sm.latestId[serverId] >= commandId{
		return true
	}else{
		sm.latestId[serverId] = commandId
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Operation:"Join", Servers:args.Servers, CommandId:args.CommandId, ClientId:args.ClientId}
	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if reply.WrongLeader == true{
		reply.Err = ErrWrongLeader
		return
	}

	sm.mu.Lock()
	ch,ok := sm.commits[index]
    if !ok{
    	sm.commits[index] = make(chan bool,1)
    	ch = sm.commits[index]
    }
	sm.mu.Unlock()

	//DPrintf("Before waiting ch")
	
	select{
	case <-ch:  // uid := <-kv.commitGet:
		DPrintf("OK get join")
		reply.Err = OK

    case <-time.After(time.Millisecond*1000):
    	reply.Err = ErrTimeOut
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Operation:"Leave", GIDs: args.GIDs, CommandId:args.CommandId, ClientId:args.ClientId}
	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if reply.WrongLeader == true{
		reply.Err = ErrWrongLeader
		return
	}

	sm.mu.Lock()
	ch,ok := sm.commits[index]
    if !ok{
    	sm.commits[index] = make(chan bool,1)
    	ch = sm.commits[index]
    }
	sm.mu.Unlock()
	
	select{
	case <-ch:  // uid := <-kv.commitGet:
		reply.Err = OK

    case <-time.After(time.Millisecond*1000):
    	
    	reply.Err = ErrTimeOut
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Operation:"Move", Shard:args.Shard, GID:args.GID, CommandId:args.CommandId, ClientId:args.ClientId}
	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if reply.WrongLeader == true{
		reply.Err = ErrWrongLeader
		return
	}

	sm.mu.Lock()
	ch,ok := sm.commits[index]
    if !ok{
    	sm.commits[index] = make(chan bool,1)
    	ch = sm.commits[index]
    }
	sm.mu.Unlock()
	
	select{
	case <-ch:  // uid := <-kv.commitGet:
		reply.Err = OK
		

    case <-time.After(time.Millisecond*1000):
    	reply.Err = ErrTimeOut
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Operation:"Query", Num:args.Num, CommandId:args.CommandId, ClientId:args.ClientId}
	index, _, isLeader := sm.rf.Start(op)
	reply.WrongLeader = !isLeader
	if reply.WrongLeader == true{
		reply.Err = ErrWrongLeader
    	return
	}

	sm.mu.Lock()

    ch,ok := sm.commits[index]
    if !ok{
    	sm.commits[index] = make(chan bool,1)
    	ch = sm.commits[index]
    }

    sm.mu.Unlock()

	select{
	case <-ch:  // uid := <-kv.commitGet:
		sm.mu.Lock()
		if args.Num == -1 || args.Num >= len(sm.configs){
			if len(sm.configs) == 0{
				reply.Err = ErrNoKey
			}else{
				reply.Config = sm.configs[len(sm.configs) -1]
				reply.Err = OK
			}
			
		}else{
			if len(sm.configs) == 0{
				reply.Err = ErrNoKey
			}else{
				reply.Config = sm.configs[args.Num]
				reply.Err = OK
			}
		}
		sm.mu.Unlock()

    case <-time.After(time.Millisecond*1000):
    	
    	reply.Err = ErrTimeOut
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) NewConfig() *Config{
	oldConfig := sm.configs[sm.configIdx]
	newconfig := Config{}
	newconfig.Groups = make(map[int][]string)
	newconfig.Num = oldConfig.Num + 1
	sm.configIdx = newconfig.Num
	for k, v := range oldConfig.Groups{
		newconfig.Groups[k] = v
	}
	for idx, v := range oldConfig.Shards{
		newconfig.Shards[idx] = v
	}
	sm.configs = append(sm.configs, newconfig)

	return &newconfig
}

func (sm *ShardMaster) GetMaxGid() int{
	shards := sm.configs[sm.configIdx].Shards
	count := map[int]int{}
	maxNum := 0
	maxShardIdx := -1
	
	for i:=0;i<len(shards);i++{
		_,ok := count[shards[i]]
		if  ok{
			count[shards[i]] = count[shards[i]] + 1
		}else{
			count[shards[i]] = 1
		}
		if count[shards[i]] > maxNum{
			maxNum = count[shards[i]]
			maxShardIdx = shards[i]
		}
	}

	return maxShardIdx
}

func (sm *ShardMaster) GetMinGid() int{
	shards := sm.configs[sm.configIdx].Shards
	count := map[int]int{}
	minNum := NShards + 1
	minShardIdx := -1
	for i:=0;i<len(shards);i++{
		_,ok := count[shards[i]]
		if  ok{
			count[shards[i]] = count[shards[i]] + 1
		}else{
			count[shards[i]] = 1
		}
	}

	for k,v := range count{
		if v<minNum{
			minNum = v
			minShardIdx = k
		}
	}
	
	return minShardIdx
}

func (sm *ShardMaster) IsExistGid(gid int) bool{
	shards := sm.configs[sm.configIdx].Shards
	for _,v := range shards{
		if v == gid{
			return true
		}
	}
	return false
}

func (sm *ShardMaster) SetGid(gid1 int, gid2 int){
	currentConfig := &sm.configs[sm.configIdx]
	for k,v := range currentConfig.Shards{
		if v == gid1{
			currentConfig.Shards[k] = gid2
			//DPrintf("currentCOnfig.Shards[%d]: gid2:%d",k, gid2)
			return
		}
	}
}

func (sm *ShardMaster) ShowShards(){
	DPrintf("%v",sm.configs[sm.configIdx].Shards)
}

func (sm *ShardMaster) GetCurrentConfigs() *Config{
	return &sm.configs[sm.configIdx]
}

func (sm *ShardMaster) FindFirstGid(start, gid int) int{
	currentConfig := sm.GetCurrentConfigs()
	for start < len(currentConfig.Shards) && currentConfig.Shards[start] != gid{
		start++
	}
	return start
}

// make sure gids in shards are balanced
func (sm *ShardMaster) CombineServers(servers *map[int][]string){
	//DPrintf("sm.configIdx:%d len(sm.configs)")
	sm.ShowShards()
	currentConfig := &sm.configs[sm.configIdx]

	for gid,server := range (*servers){
		currentConfig.Groups[gid] = server
		
	}

	sm.Balance()


	//DPrintf("NShards:%d, num:%d", NShards, num)
	//DPrintf("After set: %v",currentConfig.Shards)
}

func (sm * ShardMaster) Balance(){
	currentConfig := sm.GetCurrentConfigs()

	count := map[int]int{}
	maxGid := -1
	maxCount := -1
	storeEmpty := []int{}

	for i,gid := range currentConfig.Shards{
		_,ok := currentConfig.Groups[gid]
		if ok == false{
			currentConfig.Shards[i] = -1
			storeEmpty = append(storeEmpty, i)
		}else{
			_,ok2 := count[gid]
			if ok2{
				count[gid]++
			}else{
				count[gid] = 1
			}
			if count[gid] > maxCount{
				maxCount = count[gid]
				maxGid = gid
			}
		}
	}

	for gid,_ := range currentConfig.Groups{
		_,ok := count[gid]
		if ok == false{
			count[gid] = 0
		}
	}

	num := len(currentConfig.Groups)
	if num == 0{
		return
	}
	each := NShards/num
	topnum := NShards/num
	if NShards%num != 0{
		topnum++
	}

	if NShards < num{
		topnum = 0
	}

	if each == 0{
		each = 1
	}
	if topnum == 0{
		topnum = 1
	}

	// remove excessed gids
	for i,v := range count{
		calnum := 0
		if maxGid == -1{
			maxGid = i
		}
		if i == maxGid{
			calnum = topnum
		}else{
			calnum = each
		}

		if v > calnum{
			idx := 0
			for v > calnum{
				idx = sm.FindFirstGid(idx, i)
				
				currentConfig.Shards[idx] = -1
				storeEmpty = append(storeEmpty,idx)

				idx++
				v--
			}
		}
	}

	storeEmptyIdx := 0
	for i,v := range count{

		if storeEmptyIdx >= len(storeEmpty){
			break
		}

		calnum := 0

		if i == maxGid{
			calnum = topnum
		}else{
			calnum = each
		}

		if v == calnum{
			continue
		}

		
		for storeEmptyIdx < len(storeEmpty) && v < calnum{
			currentConfig.Shards[storeEmpty[storeEmptyIdx]] = i
			storeEmptyIdx++
			v++
		}

		
		
	}

	DPrintf("NShards:%d, num:%d", NShards, num)
	DPrintf("After Balance:%v", currentConfig.Shards)

}

func (sm * ShardMaster) RemoveGids(GIDs *[]int){
	
	currentConfig := &sm.configs[sm.configIdx]

	//DPrintf("In RemoveGids:%v", currentConfig.Shards)

	//remove gids from Groups
	for _,v := range (*GIDs){
		delete(currentConfig.Groups, v)
	}

	sm.Balance()


	DPrintf("In RemoveGids:%v", currentConfig.Shards)


}



func (sm *ShardMaster) Apply(){
	for true{
		select{
		//case <- kv.isKilled:
		//	break
		case msg := <- sm.applyCh:
			if msg.CommandValid == true{
				//DPrintf("Apply: %v, me:%v", msg, sm.me)
				command := msg.Command.(Op)
				sm.mu.Lock()
				//DPrintf("kv.latestId[%v]: %v", command.ClientId, sm.latestId[command.ClientId])

				sm.commands[msg.CommandIndex] = command
				if sm.exist(command.CommandId, command.ClientId) == false{
					switch command.Operation{
					case "Join":
						servers := command.Servers
						sm.NewConfig()
						sm.CombineServers(&servers)

					case "Leave":
						GIDs := command.GIDs
						sm.NewConfig()
						sm.RemoveGids(&GIDs)

					case "Move":
						sm.NewConfig()
						sm.configs[sm.configIdx].Shards[command.Shard] = command.GID

					case "Query":

				}
				ch, ok := sm.commits[msg.CommandIndex]
				//_, isLeader := sm.rf.GetState()

				commitMsgCh := sm.commits[msg.CommandIndex]

				sm.mu.Unlock()
				if ok{
					select{
					case <- commitMsgCh:
					default:
					}
					ch <- true
				}
				//if isLeader{
				//	sm.SendSnapshot(msg.CommandIndex)
				//}
			}else{
				sm.mu.Lock()
				//DPrintf("Receive Snapshot")
				//sm.readSnapshot(msg.SnapshotData)
				sm.mu.Unlock()
				//DPrintf("Command: %v", kv.store)
			}

			}
			
			//kv.commits[msg.CommandIndex] <- true
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	for i:=0;i<NShards;i++{
		sm.configs[0].Shards[i] = -1
	}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.latestId = make(map[int64]int)
	sm.commands = make(map[int]Op)
	sm.commits = make(map[int]chan bool)
	sm.configIdx = 0
	go sm.Apply()

	return sm
}
