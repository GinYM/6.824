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
	currentTerm int
	votedFor int
	log []*LogEntry
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

}

func (rf *Raft) showInfo(){
	DPrintf("rf.me:%d, len(rf.log):%d, commitIndex:%d, lastCommand:%v",rf.me,len(rf.log),rf.commitIndex,rf.log[len(rf.log)-1].Command)
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
}

//
// AppendEntries RPC hander
// heartbeat
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf_currentTerm := rf.currentTerm
	logLen := len(rf.log)
	rf.mu.Unlock()

	reply.Term = rf_currentTerm

	//rule 1 Reply false if term < currentTerm
	if args.Term <rf_currentTerm{
		reply.Success = false
		//DPrintf("rule1 ! term discon")
		//DPrintf("Here1")
		//DPrintf("leader:%d\n",args.LeaderId)
		return
	}

	if args.PrevLogIndex>=logLen{
		//DPrintf("Here2")
		DPrintf("prelogindex out of range")
		reply.Success = false
		//DPrintf("leader:%d\n",args.LeaderId)
		return
	}

	rf.mu.Lock()
	//Rule 2 Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm 
	//DPrintf("len of log: %d in Leader: %d args.PreLogIndex %d",len(rf.log),args.LeaderId,args.PrevLogIndex)
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
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
	

	//rule for all servers
	if args.Term > rf_currentTerm{
		//DPrintf("rf.state %d",rf.state)
		rf.mu.Lock()
		rf.state = Follower
		rf.count = 0
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}


	//Rule3 find the confict index, named as record
	record := 1
	for record=1;record<min(len(rf.log),len(args.Entries));record++{
		//DPrintf("Rule3 1")
		if rf.log[record].Term!=args.Entries[record].Term {
			break
		}
	}

	//Rule3 delete the confict element and all that follows
	if len(args.Entries) !=0{
		rf.showInfo()
		//DPrintf("len args.Entries %d",len(args.Entries))
		//DPrintf("Rule3 2")
		rf.log = rf.log[:record]
		//Rule4 append new entries
		rf.log = append(rf.log,args.Entries[record:]...)
		//DPrintf("len(rf.log): %d, rf.me: %d",len(rf.log),rf.me)
	}

	//Rule5 bug!!! len(rf.log)
	if  args.LeaderCommit > rf.commitIndex{
		//DPrintf("args.LeaderCommit %d, len(rf.log) %d, rf.me: %d",args.LeaderCommit,len(rf.log)-1,rf.me)
		rf.commitIndex = min(args.LeaderCommit,len(rf.log)-1)
		//DPrintf("len(rf.log):%d, rf.me:%d",len(rf.log),rf.me)
		for rf.lastApplied < rf.commitIndex{
			rf.lastApplied++
			//DPrintf("Sending ApplyMsg lastApplied:%d",rf.lastApplied)
			rf.applyCh<-ApplyMsg{
				CommandValid:true,
				Command:rf.log[rf.lastApplied].Command,
				CommandIndex:rf.lastApplied}
		}

		rf.showInfo()

		//DPrintf("rf.commitIndex %d, args.LeaderCommit: %d",rf.commitIndex,args.LeaderCommit)
	}


	//rf.mu.Lock()
	//reset timer for heartbeat
	if len(args.Entries) == 0{
		//rf.mu.Lock()
		//DPrintf("Reset timmer")
		rf.resetTimmer(false)
		//rf.mu.Unlock()
	}
	rf.mu.Unlock()
}





func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf_currentTerm := rf.currentTerm
	rf_votedFor := rf.votedFor
	rf_lastApplied := rf.lastApplied
	rf.mu.Unlock()
	if args.Term <rf_currentTerm{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//rf.mu.Lock()
	if args.Term > rf_currentTerm {
		rf.mu.Lock()
		rf.state = Follower
		rf.votedFor = -1
		rf.count = 0
		rf_votedFor = -1
		rf_currentTerm = args.Term
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}
	//rf.mu.Unlock()

	
	if rf_votedFor == -1 || rf_votedFor == args.CandidateId{
		//DPrintf("in votedFor")
		//DPrintf("args.Term: %d rf.currentTerm: %d",args.Term,rf.currentTerm)
		//DPrintf("args.LastLogIndex %d rf.lastApplied %d",args.LastLogIndex,rf.lastApplied)
		if (args.Term > rf_currentTerm)||(args.Term == rf_currentTerm && args.LastLogIndex>=rf_lastApplied){
			//DPrintf("from %d to %d rf.votedFor %d",rf.me,args.CandidateId,rf.votedFor)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
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
	return time.Millisecond*time.Duration(rand.Int()%151+200)
}


//heartbeat time
func duration_heartbeat() time.Duration{
	return time.Millisecond*50
}

// reset election time
func (rf * Raft) resetTimmer(isHeartbeat bool){
	
	//if !rf.timer.Stop(){
	//	<-rf.timer.C
	//}
	//DPrintf("Reset time")
	//rf.mu.Lock()
	if isHeartbeat {
		rf.timer.Reset(duration_heartbeat())
		
	}else{
		rf.timer.Reset(duration())
		//DPrintf("time: %d",duration())
	}
	//rf.mu.Unlock()
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
		//DPrintf("Here Start new command!")
		rf.log = append(rf.log,&LogEntry{command,rf.currentTerm})
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		//DPrintf("rf.nextIndex %d",rf.nextIndex[rf.me])
		//rf.nextIndex[rf.me] = len(rf.log)
		//rf.matchIndex[rf.me] = len(rf.log)-1
	}
	rf.mu.Unlock()

	//DPrintf("Leader is: %d",rf.me)
	if isLeader{
		DPrintf("sned RPC, lastCommand:%v",rf.log[len(rf.log)-1].Command)
		go rf.sendRPC()
		DPrintf("finished rpc")
	}


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
	for i:=0;i<len(rf.peers);i++{
		//DPrintf("Here?")
		if i == rf.me{
			continue
		}
		go func(rf *Raft, i int){
			rf.mu.Lock()
			args := RequestVoteArgs{rf.currentTerm,rf.me,rf.lastApplied,rf.log[rf.lastApplied].Term}
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			rf.sendRequestVote(i,&args,&reply)
			if reply.VoteGranted == true{
				rf.mu.Lock()
				rf.count++
				//DPrintf("count: %d",rf.count)
				transformToLeader := false
				if rf.count == len(rf.peers)/2+len(rf.peers)%2{
					transformToLeader = true
					rf.state = Leader
				}
				rf.mu.Unlock()

				if transformToLeader{

					go rf.sendHeartbeat()
					rf.mu.Lock()
					//initialize nextIndex after reletion
					for i:=0;i<len(rf.nextIndex);i++{
						//DPrintf("exm?")
						rf.nextIndex[i] = len(rf.log)
					}
					rf.mu.Unlock()
				}
			}
		}(rf,i)
		
	}
}

//send heartbeat to all server
func (rf *Raft) sendHeartbeat(){
	for i:=0;i<len(rf.peers);i++{
		if i == rf.me{
			continue
		}
		go func(rf *Raft, i int){
			//DPrintf("sending heartbeat")
			rf.mu.Lock()
			args:=AppendEntriesArgs{
				Term:rf.currentTerm,
				LeaderId:rf.me,
				PrevLogIndex:rf.matchIndex[rf.me],
				PrevLogTerm:rf.log[rf.matchIndex[rf.me]].Term,
				Entries:[]*LogEntry{},
				LeaderCommit:rf.commitIndex}
			rf.mu.Unlock()
			reply:=AppendEntriesReply{}
			//reset followers' timer
			rf.sendAppendEntries(i, &args, &reply)
			//DPrintf("Send heartbeat from leader: %d, reply:%t",rf.me,reply.Success)
			
		}(rf,i)
	}
	//reset leader's timer
	rf.mu.Lock()
	rf.resetTimmer(true)
	rf.mu.Unlock()
}

func (rf *Raft) sendRPC(){
	//for true{
		//msg := <-applyCh
	rf.mu.Lock()
	DPrintf("Here in sendRPC, lastCommand:%v",rf.log[len(rf.log)-1].Command)
	//If command received from client: append entry to local log,
	//respond after entry applied to state machine
	//rf.log = append(rf.log,&LogEntry{command,rf.currentTerm})
	//DPrintf("len(rf.log): %d",len(rf.log))
	lastLogIndex := len(rf.log)-1
	lenNextIndex := len(rf.nextIndex) 
	rf.mu.Unlock()

	rf.showInfo()

	for i:=0;i<lenNextIndex;i++{
		//DPrintf("len of nextindex: %d",lenNextIndex)
		if i == rf.me{
			continue
		}
		go func(rf *Raft,i int,lastLogIndex int){
			rf.mu.Lock()
			//DPrintf("Here?")
			nextIndex := min(rf.nextIndex[i],lastLogIndex+1)
			//DPrintf("NextIndex is: %d",nextIndex)
			rf.mu.Unlock()
			for true{
				DPrintf("nextIndex:%d, lastLogIndex:%d",nextIndex,lastLogIndex)

				rf.mu.Lock()
				isKilled:=rf.isKilled
				rf.mu.Unlock()
				if isKilled{
					break
				}

				//DPrintf("next index: %d",nextIndex)

				if nextIndex <1{
					rf.mu.Lock()
					rf.nextIndex[i] = 1
					rf.mu.Unlock()
					break
				}




				if lastLogIndex+1 >= nextIndex{
					//DPrintf("Here")
					rf.mu.Lock()
					//DPrintf("Leader: %d",rf.me)
					//DPrintf("AppendEntries: nextIndex: %d",nextIndex)
					//DPrintf("AppendEntries: len(rf.log): %d",len(rf.log))
					//if rf.nextIndex[i]>=len(rf.log){
					//	DPrintf("leader:%d follower:%d, nextIndex is %d, len(log): %d",rf.me,i,rf.nextIndex[i],len(rf.log))
					//}
					logLen:=len(rf.log)
					rf.showInfo()
					DPrintf("sned RPC len(rf.log):%d, lastCommand:%v, leader Commit:%d, leader:%d, i:%d",len(rf.log),rf.log[len(rf.log)-1].Command,rf.commitIndex,rf.me,i)
					args:=AppendEntriesArgs{
						Term:rf.currentTerm,
						LeaderId:rf.me,
						PrevLogIndex:nextIndex-1,
						PrevLogTerm:rf.log[nextIndex-1].Term,
						Entries:rf.log,
						LeaderCommit:rf.commitIndex}
					rf.mu.Unlock()
					reply:=AppendEntriesReply{}
					rf.sendAppendEntries(i, &args, &reply)
					//DPrintf("Before success")
					if reply.Success == true{
						//DPrintf("Here? after success")
						rf.mu.Lock()
						rf.nextIndex[i] = logLen
						//DPrintf("leader:%d follower:%d, nextIndex is %d, len(log): %d",rf.me,i,rf.nextIndex[i],len(rf.log))
						rf.matchIndex[i] = logLen-1 //bug? check matchIndex
						//DPrintf("match index: %d",rf.matchIndex[i])
						rf.mu.Unlock()
						break
					}else{
						//decrement nextIndex and retry
						rf.mu.Lock()
						nextIndex--
						//DPrintf("Here else")
						//rf.nextIndex[i]--
						rf.mu.Unlock()
					}
				}else{
					break
				}

			}
			
		}(rf,i,lastLogIndex)

		
	}

	//rf.mu.Unlock()
	//}
}

//rule for leaders, last one
func (rf *Raft) checkCommitIndex(){
	//DPrintf("Here?")
	rf.mu.Lock()
	storeMatchIndex := make([]int,len(rf.matchIndex))
	//lenPeers:=len(rf.peers)
	copy(storeMatchIndex,rf.matchIndex)
	rf_commitIndex:=rf.commitIndex
	rf.mu.Unlock()

	//DPrintf("storeMatchIndex: %d",storeMatchIndex[len(storeMatchIndex)-1])
	sort.Ints(storeMatchIndex)
	//DPrintf("match 0:%d, 1:%d, 2:%d",storeMatchIndex[0],storeMatchIndex[1],storeMatchIndex[2])
	for i:=len(storeMatchIndex)/2-(len(storeMatchIndex)+1)%2;i>=0;i--{

		if(storeMatchIndex[i]>rf_commitIndex){
			rf.mu.Lock()
			if rf.log[storeMatchIndex[i]].Term == rf.currentTerm{
				rf.commitIndex = storeMatchIndex[i]
				for rf.lastApplied < rf.commitIndex{
					rf.lastApplied++
					rf.applyCh<-ApplyMsg{
						CommandValid:true,
						Command:rf.log[rf.commitIndex].Command,
						CommandIndex:rf.lastApplied}
				}
				//DPrintf("leader:%d commitIndex: %d",rf.me,rf.commitIndex)
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()
		}else{
			break
		}
	}

	//go rf.sendHeartbeat()

	//DPrintf("Here?")

}

func (rf *Raft) startMsg(applyCh chan ApplyMsg){
	for true{
		//DPrintf("Here?")
		msg:=<-applyCh
		rf.Start(msg.Command)
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


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	go func (rf * Raft) {
		for true {
			//DPrintf("Here?")
			<-rf.timer.C
			rf.mu.Lock()
			rf_state:=rf.state
			rf.mu.Unlock()
			switch rf_state{
			case Follower:
				//if election timeout covert to candidate
				rf.mu.Lock()
				rf.state = Candidate
				rf.resetTimmer(false)
				rf.mu.Unlock()
			case Candidate:
				//DPrintf("IN candidate")
				//increment currentTerm, vote for itself, reset election time
				rf.mu.Lock()
				rf.currentTerm++
				//rf.mu.Unlock()
				//rf.mu.Lock()
				rf.votedFor = rf.me
				//rf.mu.Unlock()
				rf.resetTimmer(false)
				//send request vote to all other servers
				rf.count = 1
				rf.mu.Unlock()
				go rf.sendRequestVoteAll()
				rf.mu.Lock()
				rf.resetTimmer(false)
				rf.mu.Unlock()
			case Leader:
				//DPrintf("Here?")
				//rf.mu.Lock()
				//toLeader := rf.toLeader
				//rf.toLeader = false
				//rf.mu.Unlock()
				
				//if toLeader{
				//	go rf.startMsg(applyCh)
				//}
				go rf.checkCommitIndex()
				go rf.sendHeartbeat()
			}
		}
	}(rf)

	return rf
}
