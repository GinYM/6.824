package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
//import "fmt"
import "math/rand"
import "time"
import "sort"
import "bytes"
import "labgob"

// import "bytes"
// import "labgob"

const Follower = 0
const Candidate = 1
const Leader = 2

//min function

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

//max function

func max(a,b int) int{
	if a>b{
		return a
	}
	return b
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	SnapshotData []byte
	//UseSnapshot bool

}

type LogEntry struct {
	Command interface{}
	Term int
	Index int
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// persistent state
	currentTerm int
	votedFor int
	log []*LogEntry

	//volatile state
	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int 
	state int //Follower Candidate or Leader
	count int // number of voters
	timer *time.Timer // Timer 
	toLeader bool // if it is transmit from candidate to leader
	//duration time.Duration //duration for timer
	applyCh chan ApplyMsg
	isKilled bool // kill the raft
	isKilledChan chan bool

	//channel
	heartbeatCh chan int
	isLeaderCh chan bool
	commitCh chan int

	snapshotCh chan bool

}

func (rf *Raft) showInfo(){
	DPrintf("rf.me:%d, len(rf.log):%d, commitIndex:%d, lastTerm:%d, currentTerm:%d, vote:%d",rf.me,len(rf.log),rf.commitIndex,rf.log[len(rf.log)-1].Term,rf.currentTerm,rf.votedFor)
}

func (rf *Raft) getLastIndex() int{
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	state:=rf.state
	rf.mu.Unlock()

	//rf.mu.Lock()
	if state == Leader{
		isleader = true
	}else{
		isleader = false
	}
	//rf.mu.Unlock()


	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []*LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil{
			DPrintf("Error!")
			return
	} else{
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		
	}
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct{
	Term int
	Success bool
}

func (rf *Raft) SaveSnapshot(args *InstallSnapshotArgs) {

	w_state := new(bytes.Buffer)
	e_state := labgob.NewEncoder(w_state)
	e_state.Encode(rf.currentTerm)
	e_state.Encode(rf.votedFor)
	e_state.Encode(rf.log)
	
	data_state := w_state.Bytes()
	//rf.persister.SaveRaftState(data)

	w_snapshot := new(bytes.Buffer)
	e_snapshot := labgob.NewEncoder(w_snapshot)
	e_snapshot.Encode(args.LastIncludedIndex)
	e_snapshot.Encode(args.LastIncludedTerm)
	e_snapshot.Encode(args.Data)
	
	data_snapshot := w_snapshot.Bytes()

	rf.persister.SaveStateAndSnapshot(data_state, data_snapshot)
	//rf.persister.SaveRaftState(data)
}

func (rf *Raft) ReadSnapshotAll() (int,int,[]byte){
	snapshotData := rf.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var lastIncludedTerm int
	var data []byte

	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&data) != nil{
			DPrintf("Error!")
			return lastIncludedIndex, lastIncludedTerm, data
	} else{
		return lastIncludedIndex, lastIncludedTerm, data
	}
}

func (rf *Raft) ReadSnapshot() []byte{
	snapshotData := rf.persister.ReadSnapshot()
	r := bytes.NewBuffer(snapshotData)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var LastIncludedTerm int
	var data []byte

	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&LastIncludedTerm) != nil ||
		d.Decode(&data) != nil{
			DPrintf("Error!")
			return data
	} else{
		return data
	}
}


func (rf *Raft) RemoveOldLog(lastIndex int, lastTerm int){
	if lastIndex > rf.getLastIndex(){
		rf.log = []*LogEntry{}
		rf.log = append(rf.log,&LogEntry{Term: lastTerm, Index: lastIndex})
		//rf.log = []LogEntry{LogEntry{Term: lastTerm, Index: lastIndex}}
		//rf.log = []
		//rf.log = append(rf.log, LogEntry{Term: lastTerm, Index: lastIndex})
	}else if lastIndex < rf.log[0].Index{
		DPrintf("Already exists, lastIndex:%v, rf.log[0].Index:%v, rf.me:%v", lastIndex, rf.log[0].Index, rf.me)
		lastIndex = rf.log[0].Index
		
	}else{
		DPrintf("Before rf.log[0].Index:%v", rf.log[0].Index)
		rf.log = rf.log[rf.getRealIndex(lastIndex):]
	}

	DPrintf("lastIndex:%v, rf.log[0].Index:%v", lastIndex, rf.log[0].Index)

	if lastIndex > rf.commitIndex{
		rf.commitIndex = lastIndex
	}
	if lastIndex > rf.lastApplied{
		rf.lastApplied = lastIndex
	}

	rf.commitIndex = rf.log[0].Index // max(rf.commitIndex, rf.log[0].Index)
	rf.lastApplied = rf.log[0].Index // max(rf.lastApplied, rf.log[0].Index)

	for i:=0; i<len(rf.nextIndex); i++{
		rf.nextIndex[i] = max(rf.nextIndex[i], rf.log[0].Index+1)
	}

}

func(rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.log[0].Index {
		rf.mu.Unlock()
		return
	} 

	reply.Success = true
	rf.SaveSnapshot(args)
	rf.RemoveOldLog(args.LastIncludedIndex, args.LastIncludedTerm)
	DPrintf("rf.me:%v, rf.log[0].Index:%v, args.LastIncludedIndex:%v", rf.me, rf.log[0].Index, args.LastIncludedIndex)
	rf.mu.Unlock()
	rf.snapshotCh <- true
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool{

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for ok==false{
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if rf.isKilled == false && state == Leader{
			ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		}else{
			break
		}
	}
	return ok
}

func (rf *Raft) SendInstallSnapshotMyself(){
	rf.mu.Lock()
	lastIncludedIndex, lastIncludedTerm, data := rf.ReadSnapshotAll()
	args := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm: lastIncludedTerm,
				Data: data}
	rf.mu.Unlock()
	if len(args.Data) == 0{
		return
	}
	reply := InstallSnapshotReply{}
	rf.InstallSnapshot(&args, &reply)
	
	DPrintf("Send initial sanpshot success!, reply.Success:%v, args.lastIncludedIndex:%v, args.log[0].Index:%v", reply.Success, args.LastIncludedIndex, rf.log[0].Index)
	
}

func (rf *Raft) SendInstallSnapshotAll(index int, data []byte){
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	args_leader := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIncludedIndex: index,
				LastIncludedTerm: rf.log[rf.getRealIndex(index)].Term,
				Data: data}
	//rf.InstallSnapshot(&args,&reply)
	if index > rf.log[0].Index{
		rf.SaveSnapshot(&args_leader)
		rf.RemoveOldLog(args_leader.LastIncludedIndex, args_leader.LastIncludedTerm)
	}else{
		rf.mu.Unlock()
		return
	}
	//rf.SaveSnapshot(&args_leader)
	//rf.RemoveOldLog(args_leader.LastIncludedIndex, args_leader.LastIncludedTerm)
	leaderId := rf.me
	lenPeer := len(rf.peers)
	//lastTerm :=  rf.log[rf.getRealIndex(index)].Term
	
	DPrintf("In SendInstallSnapshotAll, rf.me:%v, index:%v, log[0].Index:%v", rf.me, index, rf.log[0].Index)

	rf.mu.Unlock()


	for i:= 0;i < lenPeer; i++{
		if i == leaderId{
			continue
		}
		go func(rf *Raft, args *InstallSnapshotArgs, i int){
			//DPrintf("Log[0].Index:%v, index:%v, len(rf.log):%v, rf.log[getRealIndex]:%v", rf.log[0].Index, index, len(rf.log), rf.log[rf.getRealIndex(index)].Term)
			
			reply := InstallSnapshotReply{}
			ok := rf.SendInstallSnapshot(i, args, &reply)
			if ok && reply.Success == true{
				DPrintf("Send Snapshot success")
				rf.mu.Lock()
				rf.nextIndex[i] = index + 1
				rf.mu.Unlock()
				//rf.snapshotCh <- true
			}
		}(rf, &args_leader, i)
	}
}

//
// AppendEntries RPC structure
// 
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int

	PrevLogTerm int
	Entries []*LogEntry

	LeaderCommit int
}

//
// AppendEntries Reply structure
//
type AppendEntriesReply struct {
	Term int
	Success bool
	InconIdx int
}

//
// AppendEntries RPC hander
// heartbeat
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	//DPrintf("Here??")
	
	rf.mu.Lock()
	
	rf_currentTerm := rf.currentTerm
	logLen := len(rf.log)
	//rf_state = rf.state
	//state := rf.state
	rf.mu.Unlock()

	reply.Term = rf_currentTerm
	reply.Success = false

	reply.InconIdx = -1

	if args.Term > rf_currentTerm{
		//DPrintf("rf.state %d",rf.state)
		rf.mu.Lock()
		//if state == Leader{
			//DPrintf("leader becombing follower rf.me:%d",rf.me)
		rf.convertToFollower(args.Term)
		reply.Term = rf.currentTerm
		//DPrintf("reply.term:%d",reply.Term)
		//}
		rf.persist()
		rf.mu.Unlock()
		
	}

	

	//rule 1 Reply false if term < currentTerm
	if args.Term <rf_currentTerm{
		reply.Success = false
		
		return
	}

	rf.mu.Lock()
	realIndex := rf.getRealIndex(args.PrevLogIndex)
	rf.mu.Unlock()

	//DPrintf("rf.me:%v, len of log:%v, args.PrevLogIndex:%v, log[0].Index:%v", rf.me, len(rf.log), args.PrevLogIndex, rf.log[0].Index)

	if realIndex >= logLen{
		reply.Success = false
		return
	}

	rf.mu.Lock()
	//Rule 2 Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm 
	//DPrintf("len of log: %d in Leader: %d args.PreLogIndex %d",len(rf.log),args.LeaderId,args.PrevLogIndex)
	//preTerm := rf.getPrevLogTerm(args.PrevLogIndex)
	//if preTerm == -1{
	//	reply.Success = true
	//	rf.mu.Unlock()
	//	return
	//}


	if rf.getPrevLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		DPrintf("Leader:%v, Follower:%v, leader.Term:%v, Follower.Term:%v", args.LeaderId, rf.me, args.PrevLogTerm, rf.getPrevLogTerm(args.PrevLogIndex))
		idx := -1
		for idx = rf.getRealIndex(args.PrevLogIndex);idx>=1;idx--{
			if rf.log[idx].Term != rf.getPrevLogTerm(args.PrevLogIndex){
				break
			}
		}
		if idx == -1{
			reply.InconIdx = rf.log[0].Index
		}else{
			reply.InconIdx = idx + rf.log[0].Index
		}
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	reply.Success = true

	//Rule3 find the confict index, named as record1, record2
	record1 := rf.getRealIndex(args.PrevLogIndex)+1
	record2 := 0
	for record1 < len(rf.log) && record2 < len(args.Entries){
		//DPrintf("First record1: %v len(log):%v, record2:%v, len(args.Entries):%v", record1, len(rf.log), record2, len(args.Entries))
		if rf.log[record1].Term != args.Entries[record2].Term{
			break
		}else{
			record1++
			record2++
		}
	}
	//DPrintf("here??")

	//Rule3 delete the confict element and all that follows
	if record2 < len(args.Entries){
		//DPrintf("Second record1: %v len(log):%v, record2:%v, len(args.Entries):%v", record1, len(rf.log), record2, len(args.Entries))
		rf.log = rf.log[:record1]
		rf.log = append(rf.log, args.Entries[record2:]...)
		//for i := record2; i<len(args.Entries);i++ {
		//	DPrintf("record1:%v, idx:%v ,rf.me:%v, leader:%v, Append: %v",record1, record1+i-record2, rf.me, args.LeaderId, (args.Entries[i].Command.(int)))
		//}
		//DPrintf("Append:%v", (args.Entries[record2:]...))
		rf.persist()
	}

	//rf.log = rf.log[: rf.getRealIndex(args.PrevLogIndex) + 1]
	//rf.log = append(rf.log,args.Entries[:]...)
	//rf.persist()	
	
	if  args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = min(args.LeaderCommit, rf.log[0].Index + len(rf.log) - 1)
		rf.commitIndex = max(rf.commitIndex, rf.log[0].Index)

		rf.mu.Unlock()
		rf.commitCh <- args.Term
	}else{
		rf.mu.Unlock()
	}
	rf.heartbeatCh<-rf_currentTerm
}


// send indefinitely
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	for ok==false{
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if rf.isKilled == false && state == Leader{
			//DPrintf("Sending msg, server:%v, leader:%v", server, rf.me)
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		}else{
			break
		}
	}
	
	return ok
}



//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}



//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//DPrintf("in requestVote")
	rf.mu.Lock()

	rf_last_term := rf.log[len(rf.log)-1].Term
	rf_last_index := len(rf.log)-1
	rf_currentTerm := rf.currentTerm
	rf_votedFor := rf.votedFor
	//rf_lastApplied := rf.lastApplied
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
	if args.Term <rf_currentTerm{
		reply.VoteGranted = false
		
		return
	}

	rf.mu.Lock()
	if args.Term > rf_currentTerm {
		rf.convertToFollower(args.Term)
		
	}
	//rf.mu.Unlock()
	//rf.mu.Lock()
	rf_votedFor = rf.votedFor
	//rf.mu.Unlock()

	//rf.showInfo()
	if rf_votedFor == -1 || rf_votedFor == args.CandidateId{
		DPrintf("leader:%d, follower:%d, leader_last_term:%d, follower_last_term:%d, leader_last_index:%d, follower_last_index:%d",args.CandidateId,rf.me, args.LastLogTerm,rf_last_term,args.LastLogIndex,rf_last_index)
		if (args.LastLogTerm > rf_last_term)||(args.LastLogTerm == rf_last_term && args.LastLogIndex>=rf_last_index){
			//DPrintf("from %d to %d rf.votedFor %d args.LastLogTerm(leader):%d follower(commited):%d",rf.me,args.CandidateId,rf.votedFor,args.LastLogTerm,rf_last_term)
			reply.VoteGranted = true
			//rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.persist()
			//rf.mu.Unlock()
		}
	}
	rf.mu.Unlock()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//election time
func duration() time.Duration{
	return time.Millisecond*time.Duration(rand.Int()%150+300)
}


//heartbeat time
func duration_heartbeat() time.Duration{
	return time.Millisecond*50
}

// reset election time
func (rf * Raft) resetTimmer(isHeartbeat bool){
	

	if isHeartbeat {
		rf.timer.Reset(duration_heartbeat())
		
	}else{
		rf.timer.Reset(duration())
		//DPrintf("time: %d",duration())
	}

}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	

	rf.mu.Lock()
	if rf.state!=Leader{
		isLeader=false
	}else{
		term = rf.currentTerm
		//index = rf.nextIndex[rf.me]

		index = rf.getLastIndex()+1

		DPrintf("Here Start new command! %v, leader:%d, currentTerm:%d, getLastIndex:%v",command,rf.me,rf.currentTerm, rf.getLastIndex())
		rf.log = append(rf.log,&LogEntry{command,rf.currentTerm, rf.getLastIndex()+1})
		rf.persist()
		DPrintf("Afer getLastIndex:%v", rf.getLastIndex())
		rf.nextIndex[rf.me] = rf.getLastIndex()+1
		rf.matchIndex[rf.me] = rf.getLastIndex()

		//DPrintf("rf.matchIndex: %v", rf.matchIndex[rf.me])
		
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.isKilled = true
	rf.mu.Unlock()
	close(rf.isKilledChan)
}

//send request vote to all server
func (rf *Raft) sendRequestVoteAll(){
	rf.mu.Lock()
	rf.count = 1
	rf_me := rf.me
	rf_state := rf.state
	rf.mu.Unlock()
	for i:=0;i<len(rf.peers);i++{

		//DPrintf("Here?")
		if i == rf_me || rf_state != Candidate{
			continue
		}
		go func(rf *Raft, i int){
			rf.mu.Lock()
			args := RequestVoteArgs{rf.currentTerm,rf.me,len(rf.log)-1,rf.log[len(rf.log)-1].Term}
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			rf.sendRequestVote(i,&args,&reply)

			if reply.VoteGranted == true{
				//DPrintf("get one point rf.me:%d",rf.me)
				rf.mu.Lock()
				rf.count++
				rf_count := rf.count
				len_peers := len(rf.peers)
				rf.mu.Unlock()
				//DPrintf("count: %d",rf.count)
				//transformToLeader := false
				if rf_count == len_peers/2+len_peers%2{

					//DPrintf("is leader! rf.me:%d, currentTerm:%d",rf.me,rf.currentTerm)
					rf.isLeaderCh<-true
				}
				//rf.mu.Unlock()
			}else{
				rf.mu.Lock()
				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.state = Follower
					DPrintf("become follower:%d in sendRequestVote",rf.me)
					rf.votedFor = -1
					rf.count = 0
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}(rf,i)
		
	}
}

func (rf *Raft) getPrevLogTerm(index int) int{
	DPrintf("LastIncludedIndex:%v", rf.log[0].Index)
	DPrintf("index:%v",index)

	if index - rf.log[0].Index < 0{
		return -1
	}else{
		return rf.log[index-rf.log[0].Index].Term
	}
}

func (rf *Raft) getRealIndex(index int) int{
	return index - rf.log[0].Index
}

func (rf *Raft) sendRPC(){
	//for true{
		//msg := <-applyCh
	rf.mu.Lock()
	
	//If command received from client: append entry to local log,
	//respond after entry applied to state machine
	//rf.log = append(rf.log,&LogEntry{command,rf.currentTerm})
	//DPrintf("len(rf.log): %d",len(rf.log))
	//lastLogIndex := len(rf.log)-1
	lenNextIndex := len(rf.nextIndex) 
	state := rf.state
	rf.mu.Unlock()

	if state != Leader{
		return
	}

	//rf.showInfo()

	for i:=0;i<lenNextIndex;i++{
		//DPrintf("len of nextindex: %d",lenNextIndex)
		rf.mu.Lock()
		rf_state := rf.state
		isKilled := rf.isKilled
		nextIndex := rf.nextIndex[i]
		lastIncludedIndex := rf.log[0].Index
		
		rf.mu.Unlock()
		if (rf_state != Leader || isKilled == true){
			break
		}
		if i == rf.me{
			continue
		}
		

		if nextIndex <= lastIncludedIndex {
			go func(rf *Raft, i int){
				rf.mu.Lock()
				args := InstallSnapshotArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					LastIncludedIndex: rf.log[0].Index,
					LastIncludedTerm: rf.log[0].Term,
					Data: rf.ReadSnapshot()}
				reply := InstallSnapshotReply{}
				rf.mu.Unlock()
				ok := rf.SendInstallSnapshot(i, &args, &reply)
				rf.mu.Lock()
				if ok && reply.Success == true{
					DPrintf("Send Snapshot success")
					rf.nextIndex[i] = rf.log[0].Index + 1
					rf.commitIndex = rf.log[0].Index
					rf.lastApplied = rf.log[0].Index
					//rf.snapshotCh <- true
				}
				rf.mu.Unlock()
			}(rf, i)
			/*
			args := InstallSnapshotArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				LastIncludedIndex: rf.log[0].Index,
				LastIncludedTerm: rf.log[0].Term,
				Data: rf.ReadSnapshot()}
			reply := InstallSnapshotReply{}
			rf.mu.Unlock()
			ok := rf.SendInstallSnapshot(i, &args, &reply)
			rf.mu.Lock()
			if ok{
				DPrintf("Send Snapshot success")
				rf.nextIndex[i] = rf.log[0].Index + 1
				rf.commitIndex = max(rf.nextIndex[i], rf.commitIndex)
				rf.lastApplied = max(rf.commitIndex, rf.lastApplied)
				//rf.snapshotCh <- true
			}
			*/
			continue
		}

		//rf.mu.Lock()

		//rf.mu.Unlock()

		go func(rf *Raft,i int){
			rf.mu.Lock()
			//DPrintf("Here?")
			
			//lastLogIndex := len(rf.log)-1
			lastLogIndex := rf.getLastIndex()
			nextIndex := min(rf.nextIndex[i], lastLogIndex + 1)
			nextIndex = max(nextIndex, rf.log[0].Index + 1)

			DPrintf("nextIndex:%v, getRealIndex:%v", nextIndex, rf.getRealIndex(nextIndex))

			//if nextIndex <= rf.log[0].Index{

			//}

			args:=AppendEntriesArgs{
					Term:rf.currentTerm,
					LeaderId:rf.me,
					PrevLogIndex:nextIndex-1,
					PrevLogTerm:rf.getPrevLogTerm(nextIndex-1),
					Entries:rf.log[rf.getRealIndex(nextIndex):],
					LeaderCommit:rf.commitIndex}
			rf.mu.Unlock()
			reply:=AppendEntriesReply{}
			DPrintf("sending rpc from leader:%d to follower:%d, currentTerm:%d, nextIndex:%v Entries:%v",rf.me,i,rf.currentTerm, nextIndex, args.Entries)
			rf.sendAppendEntries(i, &args, &reply)				
			rf.mu.Lock()
			rf_currentTerm := rf.currentTerm
			if rf_currentTerm != reply.Term{
				DPrintf("Here????")
				rf.mu.Unlock()
				return
			}

			if reply.Success == true{
				//DPrintf("reply is true!!!")
				if len(args.Entries) != 0{
					rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1 // rf.getLastIndex() + 1
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries) //rf.getLastIndex()
					DPrintf("Result: rf.matchIndex[%v]:%v",i,rf.matchIndex[i])
				}
			}else{
				if reply.Term > rf.currentTerm{
					rf.convertToFollower(reply.Term)
				}else{
					DPrintf("Changing nextIndex, leader:%v, follower:%v", rf.me, i)
					nextIndex = reply.InconIdx
					if nextIndex<1{
						nextIndex = 1
					}
					rf.nextIndex[i] = nextIndex	
				}
						
			}
			rf.mu.Unlock()

		}(rf,i)

		
	}

	//rf.mu.Unlock()
	//}
}

//rule for leaders, last one
func (rf *Raft) checkCommitIndex(){
	//DPrintf("Here? in checkCommitIndex")
	rf.mu.Lock()
	storeMatchIndex := make([]int,len(rf.matchIndex))
	copy(storeMatchIndex,rf.matchIndex)
	rf_commitIndex:=rf.commitIndex
	sort.Ints(storeMatchIndex)
	DPrintf("rf.matchIndex:%v", rf.matchIndex[:])
	for i:=len(storeMatchIndex)/2-(len(storeMatchIndex)+1)%2;i>=0;i--{
		if(storeMatchIndex[i]>rf_commitIndex){
			DPrintf("getRealIndex:%v, len log:%v", rf.getRealIndex(storeMatchIndex[i]), len(rf.log))
			if rf.log[ rf.getRealIndex(storeMatchIndex[i]) ].Term == rf.currentTerm{
				rf.commitIndex = max(storeMatchIndex[i],rf.log[0].Index)
				DPrintf("commit Index:%v", rf.commitIndex)
				rf_currentTerm := rf.currentTerm
				rf.mu.Unlock()
				rf.commitCh<- rf_currentTerm
				rf.mu.Lock()
				break
			}
		}else{
			break
		}
	}
	rf.mu.Unlock()
}


func (rf *Raft) asCandidate(){
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.count = 1
	rf.persist()
	rf.mu.Unlock()

	rf.sendRequestVoteAll()
	select{
	case isLeader := <-rf.isLeaderCh:
		if isLeader{
			DPrintf("candidate:%v become leader, currentTerm:%v", rf.me, rf.currentTerm)
	
			rf.mu.Lock()
			//DPrintf("become leader,rf.me:%d",rf.me)
				
			rf.state = Leader
			rf.persist()

			for i:=0;i<len(rf.nextIndex);i++{
				rf.nextIndex[i] = rf.getLastIndex()+1
				rf.matchIndex[i] = rf.log[0].Index
				DPrintf("rf.matchIndex[%v]:%v",i, rf.matchIndex[i])
			}

			go rf.sendRPC()

			rf.mu.Unlock()
			rf.checkCommitIndex()
			//rf.sendRPC()
		}
		
	// Rules for server, If AppendEtries RPC received from new leader: convert to Follower
	case currentTerm := <-rf.heartbeatCh:
		DPrintf("receive heartbeatCh, become follower:%d",rf.me)
		rf.mu.Lock()
		rf.convertToFollower(currentTerm)
		//rf.state = Follower
		rf.mu.Unlock()
	case <- time.After(duration()):
	}
}

func (rf *Raft) convertToFollower(term int){
	DPrintf("become follower, rf.me:%d, currentTerm:%d",rf.me, rf.currentTerm)
	rf.currentTerm = term
	rf.state = Follower
	rf.count = 0
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) asLeader(){
	
	time.Sleep(duration_heartbeat())
	rf.checkCommitIndex()
	
	DPrintf("sending heartbeat in rf.me:%d",rf.me)

	rf.sendRPC()
	
}

func (rf *Raft) asFollower(){
	select{
	case <-time.After(duration()):
		rf.mu.Lock()
		rf.state = Candidate
		rf.mu.Unlock()
		
	case <-rf.heartbeatCh:
	}
}

func (rf *Raft) applyMsg(){
	for true{
		select{
		case <- rf.isKilledChan:
			return

		case  <-rf.commitCh:
			rf.mu.Lock()
			for i:=rf.lastApplied+1;i<=min(rf.commitIndex, rf.log[0].Index + len(rf.log)-1);i++{
				//if rf.log[i].Term != t{
				//	continue
				//}
				//DPrintf("sending message rf.me:%d, command:%v, CommandIndex:%d, state:%v, currentTerm:%d",rf.me,rf.log[rf.getRealIndex(i)].Command,i,rf.state,rf.currentTerm)
				
				index := max(i,rf.log[0].Index)

				msg := ApplyMsg{CommandValid:true,
					Command:rf.log[rf.getRealIndex(index)].Command,
					CommandIndex:index}

				var isValid bool
				if rf.getRealIndex(index) == 0{
					isValid = false
				}else{
					isValid = true
				}

				rf.mu.Unlock()

				if isValid{
					rf.applyCh <- msg
				}

				//rf.applyCh <- msg
				rf.mu.Lock()
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
		case <- rf.snapshotCh:
			rf.mu.Lock()
			msg := ApplyMsg{CommandValid:false,
					//Command:interface{},
					CommandIndex:0,
					SnapshotData: rf.ReadSnapshot()}
			rf.mu.Unlock()
			rf.applyCh <- msg
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("Initial at Make, rf.me:%v", me)
	//DPrintf("len of peers %d",len(peers))
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.isKilled = false

	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*LogEntry{}
	rf.log = append(rf.log,&LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int,len(peers)) 
	rf.toLeader = true
	for i:=0;i<len(peers);i++{
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int,len(peers)) 
	for i:=0;i<len(rf.matchIndex);i++{
		rf.matchIndex[i] = 0
	}
	rf.state = Follower
	rf.timer = time.NewTimer(duration())
	rf.count = 0

	rf.heartbeatCh = make(chan int)
	rf.isLeaderCh = make(chan bool)
	rf.commitCh = make(chan int)

	rf.snapshotCh = make(chan bool)

	rf.isKilledChan = make(chan bool)

	go rf.applyMsg()

	rf.readPersist(persister.ReadRaftState())
	rf.SendInstallSnapshotMyself()


	go func (rf * Raft) {
		for true {
			select{
			case <- rf.isKilledChan:
				return
			default:
				rf.mu.Lock()
				rf_state:=rf.state
				rf.mu.Unlock()
				switch rf_state{
				case Follower:
					rf.asFollower()
				case Candidate:
					rf.asCandidate()
				case Leader:
					rf.asLeader()
				}
			}
			
		}
	}(rf)

	return rf
}
