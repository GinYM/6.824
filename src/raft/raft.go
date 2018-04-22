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
}

type LogEntry struct {
	Command interface{}
	Term int
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

	//channel
	heartbeatCh chan bool
	isLeaderCh chan bool
	commitCh chan int

}

func (rf *Raft) showInfo(){
	DPrintf("rf.me:%d, len(rf.log):%d, commitIndex:%d, lastTerm:%d, currentTerm:%d, vote:%d",rf.me,len(rf.log),rf.commitIndex,rf.log[len(rf.log)-1].Term,rf.currentTerm,rf.votedFor)
	//for i:=1;i<=rf.lastApplied;i++{
	//	DPrintf("log[%d]:%v",i,rf.log[i].Command)
	//}
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
	//var commitIndex int
	//var lastApplied int
	//var nextIndex []int
	//var matchIndex [] int

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



	//DPrintf("rf.currentTerm:%d, args.Term:%d",rf.currentTerm,args.Term)

	reply.Term = rf_currentTerm
	reply.Success = false

	reply.InconIdx = -1

	//DPrintf("rf.me%d, reply.Term:%d",rf.me,reply.Term)

	//rule for all servers
	//DPrintf("rf.me:%d, rf.commitIndex:%d, args.LeaderCommit:%d isLeader:%d",rf.me,rf.commitIndex,args.LeaderCommit,rf.state)
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
		//rf.state = Follower
		//rf.count = 0
		//DPrintf("rf.me:%d from leader:%d, currentTerm:%d to Term:%d",rf.me,args.LeaderId,rf.currentTerm,args.Term)
		
	}

	

	//rule 1 Reply false if term < currentTerm
	if args.Term <rf_currentTerm{
		reply.Success = false
		
		return
	}

	if args.PrevLogIndex>=logLen{
		//DPrintf("Here2")
		//DPrintf("prelogindex out of range, rf.me:%d",rf.me)
		reply.Success = false
		//DPrintf("leader:%d\n",args.LeaderId)
		return
	}

	rf.mu.Lock()
	//Rule 2 Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm 
	//DPrintf("len of log: %d in Leader: %d args.PreLogIndex %d",len(rf.log),args.LeaderId,args.PrevLogIndex)
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		idx := -1
		for idx = args.PrevLogIndex;idx>=1;idx--{
			if rf.log[idx].Term != rf.log[args.PrevLogIndex].Term{
				break
			}
		}
		reply.InconIdx = idx
		//DPrintf("args.PrevLogIndex:%d args.PrevLogTerm:%d rf_Term:%d",args.PrevLogIndex,args.PrevLogTerm,rf.log[args.PrevLogIndex].Term)
		//DPrintf("Here3")
		reply.Success = false
		rf.mu.Unlock()
		//DPrintf("Term mismatch")
		//DPrintf("leader:%d\n",args.LeaderId)
		return
	}
	reply.Success = true

	//DPrintf("leader:%d",args.LeaderId)
	


	//Rule3 find the confict index, named as record1, record2
	/*
	record1 := args.PrevLogIndex+1
	record2 := 0
	for record1<len(rf.log) && record2<len(args.Entries){
		//DPrintf("Rule3 1")
		//DPrintf("rf.log term:%v, args.Entries term:%v",rf.log[record1].Term,args.Entries[record2].Term)
		if rf.log[record1].Term!=args.Entries[record2].Term {
			break
		}
		record1++
		record2++
	}
	*/



	//Rule3 delete the confict element and all that follows
	//if len(args.Entries) !=0{
		//DPrintf("Here")
		
		//DPrintf("len args.Entries %d",len(args.Entries))
		//DPrintf("Rule3 2")
		rf.log = rf.log[:args.PrevLogIndex+1]
		//Rule4 append new entries
		rf.log = append(rf.log,args.Entries[:]...)
		rf.persist()
		//DPrintf("follower:%d, leader:%d, record1:%d, record2:%d, rf_log_last_command:%v",rf.me,args.LeaderId,record1,record2,rf.log[len(rf.log)-1].Command)
		//rf.showInfo()
		//DPrintf("len(rf.log): %d, rf.me: %d",len(rf.log),rf.me)
	//}

	//Rule5 bug!!! len(rf.log)
	
	if  args.LeaderCommit > rf.commitIndex{
		
		rf.commitIndex = min(args.LeaderCommit,len(rf.log)-1)
		rf.mu.Unlock()
		rf.commitCh <- args.Term
		
	}else{
		rf.mu.Unlock()
	}
	//rf.mu.Unlock()
	rf.heartbeatCh<-true
	
}




// send indefinitely
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	for ok==false{
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if rf.isKilled == false && state == Leader{
			ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		}else{
			break
		}
	}
	
	return ok
}


// func (rf *Raft) AppendEntriesRPC


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

	//rf.mu.Lock()
	if args.Term > rf_currentTerm {
		rf.convertToFollower(args.Term)
		
	}
	//rf.mu.Unlock()
	rf.mu.Lock()
	rf_votedFor = rf.votedFor
	rf.mu.Unlock()

	//rf.showInfo()
	if rf_votedFor == -1 || rf_votedFor == args.CandidateId{
		//DPrintf("in votedFor")
		
		//DPrintf("args.LastLogIndex %d rf.lastApplied %d",args.LastLogIndex,rf.lastApplied)
		//DPrintf("leader:%d, follower:%d, leader_last_term:%d, follower_last_term:%d, leader_last_index:%d, follower_last_index:%d",args.CandidateId,rf.me, args.LastLogTerm,rf_last_term,args.LastLogIndex,rf_last_index)
		if (args.LastLogTerm > rf_last_term)||(args.LastLogTerm == rf_last_term && args.LastLogIndex>=rf_last_index){
			//DPrintf("from %d to %d rf.votedFor %d args.LastLogTerm(leader):%d follower(commited):%d",rf.me,args.CandidateId,rf.votedFor,args.LastLogTerm,rf_last_term)
			reply.VoteGranted = true
		
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.mu.Unlock()
			//rf.heartbeatCh <- true
		}
	}


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
		index = rf.nextIndex[rf.me]
		DPrintf("Here Start new command! %v, leader:%d, currentTerm:%d",command,rf.me,rf.currentTerm)
		rf.log = append(rf.log,&LogEntry{command,rf.currentTerm})
		rf.persist()
		rf.nextIndex[rf.me] = len(rf.log)
		rf.matchIndex[rf.me] = len(rf.log)-1
		
	}
	rf.mu.Unlock()

	//DPrintf("Leader is: %d",rf.me)
	//if isLeader{
		//DPrintf("sned RPC, lastCommand:%v",rf.log[len(rf.log)-1].Command)
	//	go rf.sendRPC()
		//DPrintf("finished rpc")
	//}


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
}

//send request vote to all server
func (rf *Raft) sendRequestVoteAll(){
	rf.mu.Lock()
	rf.count = 1
	rf.mu.Unlock()
	for i:=0;i<len(rf.peers);i++{

		//DPrintf("Here?")
		if i == rf.me || rf.state != Candidate{
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

					DPrintf("is leader! rf.me:%d, currentTerm:%d",rf.me,rf.currentTerm)
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
		rf.mu.Unlock()
		if (rf_state != Leader || isKilled == true){
			break
		}
		if i == rf.me{
			continue
		}
		go func(rf *Raft,i int){
			rf.mu.Lock()
			//DPrintf("Here?")
			
			lastLogIndex := len(rf.log)-1
			nextIndex := min(rf.nextIndex[i],lastLogIndex+1)

			args:=AppendEntriesArgs{
					Term:rf.currentTerm,
					LeaderId:rf.me,
					PrevLogIndex:nextIndex-1,
					PrevLogTerm:rf.log[nextIndex-1].Term,
					Entries:rf.log[nextIndex:],
					LeaderCommit:rf.commitIndex}
			rf.mu.Unlock()
			reply:=AppendEntriesReply{}
			//DPrintf("sending rpc from leader:%d to follower:%d, currentTerm:%d",rf.me,i,rf.currentTerm)
			rf.sendAppendEntries(i, &args, &reply)
			//fix respond
			//DPrintf("ok is:%v",ok)
			//DPrintf("leader:%d from follower:%d, reply.Term:%d, currentTerm:%d",rf.me,i,reply.Term,rf.currentTerm)
					
			rf.mu.Lock()
			rf_currentTerm := rf.currentTerm
			//rf.mu.Unlock()

			if rf_currentTerm != reply.Term{
				rf.mu.Unlock()
				return
			}

			if reply.Success == true{
						
				rf.nextIndex[i] =  args.PrevLogIndex+len(args.Entries)+1
				//rf.nextIndex[i] =  logLen
				//DPrintf("leader:%d follower:%d, nextIndex is %d, len(log): %d",rf.me,i,rf.nextIndex[i],len(rf.log))
				rf.matchIndex[i] = args.PrevLogIndex+len(args.Entries)
				//rf.matchIndex[i] = logLen-1 //bug? check matchIndex
				//DPrintf("rf.matchIndex:%v, nextIndex:%v",rf.matchIndex,rf.nextIndex)
				//DPrintf("match index: %d for raft:%d, leader:%d",rf.matchIndex[i],i,rf.me)
				//rf.mu.Unlock()	
			}else{
				//rf.mu.Lock()
				//DPrintf("here!!!!!")
				if reply.Term > rf.currentTerm{
					rf.convertToFollower(reply.Term)
				}else{
					//rf.mu.Lock()
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
	//lenPeers:=len(rf.peers)
	copy(storeMatchIndex,rf.matchIndex)
	rf_commitIndex:=rf.commitIndex
	//rf.mu.Unlock()
	//DPrintf("Start checking, rf.me:%d",rf.me)

	//DPrintf("storeMatchIndex: %d",storeMatchIndex[len(storeMatchIndex)-1])
	sort.Ints(storeMatchIndex)
	//DPrintf("storeMatchIndex:%v",storeMatchIndex)
	//DPrintf("match 0:%d, 1:%d, 2:%d",storeMatchIndex[0],storeMatchIndex[1],storeMatchIndex[2])
	for i:=len(storeMatchIndex)/2-(len(storeMatchIndex)+1)%2;i>=0;i--{
		//DPrintf("here in for loop")
		if(storeMatchIndex[i]>rf_commitIndex){
			//rf.mu.Lock()
			if rf.log[storeMatchIndex[i]].Term == rf.currentTerm{
				//rf.commitIndex = storeMatchIndex[i]
				//DPrintf("match find!!! commitIndex:%d",storeMatchIndex[i])
				//DPrintf("matchIndex[i]:%d rf.commitIndex: %d",storeMatchIndex[i],rf.commitIndex)
				rf.commitIndex = storeMatchIndex[i]
				rf_currentTerm := rf.currentTerm
				rf.mu.Unlock()
				rf.commitCh<- rf_currentTerm
				rf.mu.Lock()
			
				break
			}
			//rf.mu.Unlock()
		}else{
			break
		}
	}
	rf.mu.Unlock()
}


func (rf *Raft) asCandidate(){
	rf.mu.Lock()
	//DPrintf("As candidate:%d, currentTerm:%d",rf.me,rf.currentTerm)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.count = 1
	rf.persist()
	rf.mu.Unlock()

	rf.sendRequestVoteAll()
	select{
	case isLeader := <-rf.isLeaderCh:
		if isLeader{
	
			rf.mu.Lock()
			//DPrintf("become leader,rf.me:%d",rf.me)
				
			rf.state = Leader
			rf.persist()

			for i:=0;i<len(rf.nextIndex);i++{
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}

			rf.mu.Unlock()
			rf.checkCommitIndex()
			rf.sendRPC()
		}
		
		
	case <-rf.heartbeatCh:
		DPrintf("receive heartbeatCh, become follower:%d",rf.me)
		rf.mu.Lock()
		rf.state = Follower
		rf.mu.Unlock()
	case <- time.After(duration()):
		//DPrintf("election timeout for candidate rf.me:%d",rf.me)
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
		//DPrintf("election timeout rf.me:%d",rf.me)
		rf.mu.Lock()
		rf.state = Candidate
		rf.mu.Unlock()
		
	case <-rf.heartbeatCh:
		//DPrintf("receive heartbeat rf.me:%d",rf.me)
	}
}

func (rf *Raft) applyMsg(){
	for true{
		select{
		case  <-rf.commitCh:
			rf.mu.Lock()
			for i:=rf.lastApplied+1;i<=rf.commitIndex;i++{
				//if rf.log[i].Term != t{
				//	continue
				//}
				DPrintf("sending message rf.me:%d, command:%v, CommandIndex:%d, state:%v, currentTerm:%d",rf.me,rf.log[i].Command,i,rf.state,rf.currentTerm)
				msg := ApplyMsg{CommandValid:true,
					Command:rf.log[i].Command,
					CommandIndex:i}

				rf.mu.Unlock()

				rf.applyCh <- msg
				rf.mu.Lock()

				//rf.applyCh<-ApplyMsg{
				//	CommandValid:true,
				//	Command:rf.log[i].Command,
				//	CommandIndex:i}
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
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
	//DPrintf("Initial at Make")
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
	//rand.Seed(rf.me)
	//rf.duration = time.Millisecond*time.Duration(rand.Int()%151+200)
	rf.timer = time.NewTimer(duration())
	rf.count = 0

	rf.heartbeatCh = make(chan bool)
	rf.isLeaderCh = make(chan bool)
	rf.commitCh =make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	go func (rf * Raft) {
		for true {
			//DPrintf("Here?")
			//<-rf.timer.C
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
	}(rf)

	go rf.applyMsg()

	return rf
}
