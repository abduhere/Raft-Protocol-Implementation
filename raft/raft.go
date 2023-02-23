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

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
	// "fmt"
)

import "bytes"
import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

type LogEntryStruct struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	voteCount       int
	state           int
	leader          int
	electionTimeout time.Duration
	heartbeat       time.Duration
	lastHeartbeat   time.Time
	counter         int

	log         []LogEntryStruct
	applyCh     chan ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	isleader = false
	rf.mu.Lock()
	if rf.state == 1 {
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
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
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
	} else {
		if rf.currentTerm < args.Term {
			rf.votedFor = -1
			rf.state = -1
		}
		rf.currentTerm = args.Term
		reply.Term = args.Term
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm != rf.log[len(rf.log)-1].Term && args.LastLogTerm > rf.log[len(rf.log)-1].Term) || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
		rf.persist()
		// rf.heartbeat = time.Now().Nanosecond() / 1000000
		rf.lastHeartbeat = time.Now()
	}
	rf.mu.Unlock()
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntryStruct
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	} else {
		rf.state = -1
		reply.Success = true
		reply.Term = args.Term
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.leader = args.LeaderId
		}

		rf.log = args.Entries
		l := len(rf.log)

		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > rf.log[l-1].Index {
				rf.commitIndex = rf.log[l-1].Index
			} else {
				rf.commitIndex = args.LeaderCommit
			}

			// sending commited entries to client
			// go func() {
			// 	rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied = rf.lastApplied + 1
				commandToClient := ApplyMsg{Index: rf.log[rf.lastApplied].Index, Command: rf.log[rf.lastApplied].Command}
				rf.applyCh <- commandToClient
			}
			// 	rf.mu.Unlock()
			// } ()

		}
		rf.persist()
		rf.lastHeartbeat = time.Now()
	}
	// rf.heartbeat = time.Now().Nanosecond() / 1000000
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// code to send a AppendEntries RPC to a server
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	isLeader := false

	rf.mu.Lock()
	if rf.state == 1 {
		term = rf.currentTerm
		isLeader = true
		index = rf.log[len(rf.log)-1].Index + 1
		rf.log = append(rf.log, LogEntryStruct{index, term, command})
		rf.persist()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.electionTimeout = time.Duration(rand.Intn(600-300)+300) * time.Millisecond
	// rf.heartbeat = int(time.Now().UnixMilli())
	rf.heartbeat = 50 * time.Millisecond
	rf.lastHeartbeat = time.Now()
	rf.voteCount = 0
	rf.state = -1
	rf.counter = 0
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.log = make([]LogEntryStruct, 0)
	rf.log = append(rf.log, LogEntryStruct{Index: 0, Term: 0})
	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState())
	
	// Your initialization code here.
	go receivePeriodicHeartbeat(rf)
	// initialize from state persisted before a crash

	return rf
}

// function to receive periodic heartbeats and if not received before election timeout, perform election
func receivePeriodicHeartbeat(rf *Raft) {
	for {
		rf.mu.Lock()
		// heartbeat := rf.heartbeat
		electionTimeout := rf.electionTimeout
		rf.mu.Unlock()
		time.Sleep(electionTimeout)
		rf.mu.Lock()
		if rf.state == -1 {
			if (time.Duration(time.Now().Sub(rf.lastHeartbeat))) >= (electionTimeout) {
				go performElection(rf)
			}
		} else if rf.state == 0 {
			go performElection(rf)
		}
		rf.mu.Unlock()
	}
}

// function to send periodic heartbeats when elected as leader
func sendPeriodicHeartbeat(rf *Raft) {
	for {
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		reply := make([]AppendEntriesReply, len(rf.peers))
		if rf.state == 1 {
			if len(rf.peers) == 1 {
				rf.state = -1
				rf.persist()
				// rf.mu.Unlock()
				// break
			}
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					args := AppendEntriesArgs{currentTerm, rf.me, rf.nextIndex[i] - 1, rf.log[rf.nextIndex[i]-1].Term, rf.log, rf.commitIndex}
					go func(i int, args AppendEntriesArgs, reply *AppendEntriesReply) {
						if rf.sendAppendEntries(i, args, reply) {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								rf.state = -1
								rf.currentTerm = reply.Term
								rf.persist()
								rf.mu.Unlock()
								return
							} else {
								if reply.Success {
									rf.matchIndex[i] = args.Entries[len(args.Entries)-1].Index
									rf.nextIndex[i] = rf.matchIndex[i] + 1

									for j := rf.log[len(rf.log)-1].Index; j >= rf.commitIndex; j-- {
										count := 1
										for k := 0; k < len(rf.matchIndex); k++ {
											if k != rf.me && rf.matchIndex[k] >= j {
												count++
											}
											if count > len(rf.matchIndex)/2 && rf.log[j].Term == rf.currentTerm {
												rf.commitIndex = j
												for rf.commitIndex > rf.lastApplied {
													rf.lastApplied = rf.lastApplied + 1
													commandToClient := ApplyMsg{Index: rf.log[rf.lastApplied].Index, Command: rf.log[rf.lastApplied].Command}
													rf.applyCh <- commandToClient
												}
												break
											}
										}
									}
								}
							}
							rf.mu.Unlock()
						}
					}(i, args, &reply[i])
				}
			}
		} else {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(rf.heartbeat)
	}
}

// function to perform election
func performElection(rf *Raft) {
	rf.mu.Lock()
	rf.state = 0
	rf.currentTerm++
	rf.voteCount = 1
	rf.votedFor = rf.me
	request := RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term}
	reply := make([]RequestVoteReply, len(rf.peers))
	reply[rf.me].VoteGranted = true
	reply[rf.me].Term = rf.currentTerm
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, request RequestVoteArgs, reply *RequestVoteReply) {
				if rf.sendRequestVote(i, request, reply) {
					rf.mu.Lock()
					if reply.VoteGranted {
						rf.voteCount = rf.voteCount + 1
					}
					if (rf.voteCount > (len(rf.peers) / 2)) && rf.state != 1 {
						rf.state = 1
						rf.persist()
						initializeNextMatchIndices(rf)
						go sendPeriodicHeartbeat(rf)
					}
					rf.mu.Unlock()
				}
			}(i, request, &reply[i])
		}
	}
	rf.persist()
	rf.mu.Unlock()
}

func initializeNextMatchIndices(rf *Raft) {
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex = append(rf.matchIndex, 0)
		rf.nextIndex = append(rf.nextIndex, rf.log[len(rf.log)-1].Index+1)
	}
}
