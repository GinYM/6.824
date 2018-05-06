package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	commandId int
	clientId int64
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.commandId = 0
	ck.clientId	= nrand()
	//DPrintf("clientId: %v", ck.clientId)
	ck.lastLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//DPrintf("ck.clientId: %v", ck.clientId)
	
	ck.commandId++
	args := GetArgs{Key:key, Id:ck.commandId, ClientId:ck.clientId}
	var reply GetReply
	//reply := GetReply{}
	//DPrintf("lastLeader: %v", ck.lastLeader)
	i := ck.lastLeader
	for true{
		reply = GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		DPrintf("In Server: %v, Get:%v reply: %v", i, key, reply)

		if reply.Err == OK && reply.WrongLeader == false && ok == true{
			ck.lastLeader = i
			break
		//}else if reply.Err == ErrNoKey{
		//	time.Sleep(time.Millisecond*200)
		//	continue
		}else{
			time.Sleep(100*time.Millisecond)
			i = (i+1)%(len(ck.servers))
		}
	}

	if reply.Err == ErrNoKey{
		return ""
	}else{
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//DPrintf("ck.clientId: %v", ck.clientId)
	ck.commandId++
	args := PutAppendArgs{Key:key, Value:value, Op:op, Id: ck.commandId, ClientId: ck.clientId}
	i := ck.lastLeader
	var reply PutAppendReply
	for true{
		reply = PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("In server: %v, key:%v, value:%v, ok: %v reply: %v", i, key, value, ok, reply)
		
		if ok == true && reply.Err == OK && reply.WrongLeader == false{
			ck.lastLeader = i
			break
		}else{
			i = (i+1)%(len(ck.servers))
		}
	}
	
	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
