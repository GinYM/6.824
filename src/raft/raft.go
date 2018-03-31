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
	//duration time.Duration //duration for timer

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

	if args.Term <rf.currentTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm{
		//DPrintf("rf.state %d",rf.state)
		rf.mu.Lock()
		rf.state = Follower
		rf.count = 0
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	}


	record := 0
	for record=0;record<min(len(rf.log),len(args.Entries));record++{
		if rf.log[record].Term!=args.Entries[record].Term || rf.log[record].Command != args.Entries[record].Command{
			break
		}
	}

	if len(args.Entries) !=0{
		rf.log = rf.log[:record]
		rf.log = append(rf.log,args.Entries[record:]...)
	}

	if args.LeaderCommit > rf.commitIndex{
		rf.commitIndex = min(args.LeaderCommit,len(args.Entries)-1)
	}

	rf.mu.Lock()
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
	rf.mu.Unlock()
	if args.Term <rf_currentTerm{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	//rf.mu.Lock()
	if args.Term > rf.currentTerm {
		
		rf.state = Follower
		rf.votedFor = -1
		rf.count = 0
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()
	}
	//rf.mu.Unlock()

	
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		//DPrintf("in votedFor")
		//DPrintf("args.Term: %d rf.currentTerm: %d",args.Term,rf.currentTerm)
		//DPrintf("args.LastLogIndex %d rf.lastApplied %d",args.LastLogIndex,rf.lastApplied)
		if (args.Term > rf.currentTerm)||(args.Term == rf.currentTerm && args.LastLogIndex>=rf.lastApplied){
			//DPrintf("from %d to %d rf.votedFor %d",rf.me,args.CandidateId,rf.votedFor)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
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
}

//send request vote to all server
func (rf *Raft) sendRequestVoteAll(){
	for i:=0;i<len(rf.peers);i++{
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
				if rf.count >= len(rf.peers)/2+len(rf.peers)%2{
					rf.state = Leader
					//rf.sendHeartbeat()
				}
				rf.mu.Unlock()
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
			rf.sendAppendEntries(i, &args, &reply)
			
		}(rf,i)
	}
	rf.mu.Lock()
	rf.resetTimmer(true)
	rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*LogEntry{}
	rf.log = append(rf.log,&LogEntry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int,len(peers)) 
	for i:=0;i<len(peers);i++{
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int,len(peers)) 
	rf.state = Follower
	//rand.Seed(rf.me)
	//rf.duration = time.Millisecond*time.Duration(rand.Int()%151+200)
	rf.timer = time.NewTimer(duration())
	rf.count = 0


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	go func (rf * Raft) {
		for true {
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
				go rf.sendHeartbeat()
			}
		}
	}(rf)

	return rf
}
