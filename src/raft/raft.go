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
	"fmt"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

const electionTimeoutMinMs = 300
const electionTimeoutMaxMs = 600
const heartBeatTimeoutMs = 110

// import "bytes"
// import "labgob"

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

// logEntry
type LogEntry struct {
	TermId  int // term that is received by leader
	Command string
}

// Server State
type ServerState int64

const (
	Follower  ServerState = 0
	Candidate             = 1
	Leader                = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Election
	followingLeader bool // Follower下, 是否在超时周期内收到心跳

	// Lab 2
	// 0. ServerState
	serverState ServerState
	// 1. Persistent state on all servers
	//    updated on stable storage before responding to RPCs
	currentTerm int         // last term server has seen (init to 0 on first boot)
	votedFor    int         // candidateId that voted in current term
	log         []*LogEntry // log entries, each entry contains cmd for state machine

	// 2. Volatile state on all servers
	commitIndex int // index of the highest log entry known to be committed (init 0)
	lastApplied int // index of the highest log entry applied to state machine (init 0)

	// 3. Volatile state on leaders
	//    re-initialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (init to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// volatile state on followers
	lastStartWaiting int64
	electionTimeout  int64
	followerCounter  int
}

/** rf.mu.Lock() before use */
func (rf *Raft) SwitchState(s ServerState) {
	switch s {
	case Follower:
		// 切到Follower, 开启一个count超时的例程
		rf.serverState = Follower
		rf.followerCounter++
		fc := rf.followerCounter
		rf.lastStartWaiting = time.Now().UnixNano() / 1e6
		rf.electionTimeout = int64(RandInt(electionTimeoutMinMs, electionTimeoutMaxMs))
		rf.mu.Unlock()

		go func(followingCounter int) {
			for {
				rf.mu.Lock()
				if rf.killed() || rf.serverState != Follower || followingCounter != rf.followerCounter {
					rf.mu.Unlock()
					return
				}

				if time.Now().UnixNano()/1e6-rf.lastStartWaiting > rf.electionTimeout {
					rf.printfLn("Timeout check time from %d", rf.lastStartWaiting)
					if rf.serverState == Follower {
						if !rf.followingLeader {
							rf.SwitchState(Candidate)
							return
						} else {
							rf.followingLeader = false
						}
						rf.mu.Unlock()
					} else {
						// !Follower
						rf.mu.Unlock()
						return
					}

				} else {
					// 未超时
					rf.mu.Unlock()
					time.Sleep(10 * time.Millisecond)
				}

			}
		}(fc)
	case Candidate:
		// 切到Candidate, 开启一个`requestVote`的例程
		// 一定是`Follower => Candidate`
		rf.currentTerm++
		rf.votedFor = -1 // 重置votedFor
		rf.serverState = Candidate
		rf.printfLn("Candidate Term %d", rf.currentTerm)
		rf.mu.Unlock()
		go func() {
			// request vote
			rf.mu.Lock()
			rf.votedFor = rf.me // 选举自己
			// 重置选举计时器, 时间
			rf.electionTimeout = int64(RandInt(electionTimeoutMinMs, electionTimeoutMaxMs))
			rf.lastStartWaiting = time.Now().UnixNano() / 1e6
			rf.mu.Unlock()

			// 并发地对其他所有参与者发出 RequestVoted
			voteCount := 0
			voteTerm := rf.currentTerm

			for index, _ := range rf.peers {
				if index == rf.me {
					continue
				}
				go func(i int) {
					rf.mu.Lock()
					requestVoteArg := RequestVoteArgs{
						Term:        rf.currentTerm,
						CandidateId: rf.me,
						// TODO: 2b
						LastLogIndex: 0,
						LastLogTerm:  0,
					}
					rf.mu.Unlock()

					requestVoteReply := RequestVoteReply{}
					rf.printfLn("Sending RequestVote to %d", i)
					rf.sendRequestVote(i, &requestVoteArg, &requestVoteReply)
					if requestVoteReply.VoteGranted {
						// 得到选票
						rf.printfLn("Get Vote from %d", i)
						rf.mu.Lock()
						voteCount++
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						if requestVoteReply.Term > rf.currentTerm {
							voteTerm = requestVoteReply.Term
						}
						rf.mu.Unlock()
					}
				}(index)
			}

			for true {
				time.Sleep(5 * time.Millisecond)
				// 还是candidate?
				rf.mu.Lock()
				if rf.serverState != Candidate {
					rf.mu.Unlock()
					return
				}
				// 检查是否超时
				if time.Now().UnixNano()/1e6-rf.lastStartWaiting > rf.electionTimeout {
					// 超时，重新开始选举
					rf.SwitchState(Candidate)
					return
				}
				// 未超时，是否选够Majority
				if voteTerm > rf.currentTerm {
					// term更新，-> Follower
					rf.printfLn("Candidate -> Follower")
					rf.currentTerm = voteTerm
					rf.SwitchState(Follower)
					return
				}
				rf.printfLn("with %d votes", voteCount)
				if voteCount > len(rf.peers)/2-1 {
					// 选够，自己成为Leader
					rf.printfLn("Win with %d votes", voteCount)
					rf.SwitchState(Leader)
					return
				}

				rf.mu.Unlock()
			}
		}()
	case Leader:
		rf.serverState = Leader
		rf.printfLn("Become Leader")
		rf.mu.Unlock()

		go func(currentTerm int) {
			// leader
			// 发送心跳
			for {
				rf.mu.Lock()
				if rf.killed() || rf.serverState != Leader || currentTerm != rf.currentTerm {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				for index, _ := range rf.peers {
					if index == rf.me {
						continue
					}
					go func(i int) {
						rf.mu.Lock()
						arg := AppendEntriesArgs{
							Term:     currentTerm,
							LeaderId: rf.me,
						}
						rf.mu.Unlock()
						reply := AppendEntriesReply{}
						rf.sendAppendEntries(i, &arg, &reply)
					}(index)
				}
				time.Sleep(heartBeatTimeoutMs * time.Millisecond)
			}

		}(rf.currentTerm)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.serverState == Leader

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

/* AppendEntries */
type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// Receiver implementation:
	// 1. 如果 leader 的任期小于自己的任期返回 false。(5.1)
	// 2. 如果自己不存在索引、任期和 prevLogIndex、prevLogItem 匹配的日志返回 false。(5.3)
	// 3. 如果存在一条日志索引和 prevLogIndex 相等， 但是任期和 prevLogItem 不相同的日志， 需要删除这条日志及所有后继日志。（5.3）
	// 4. 如果 leader 复制的日志本地没有，则直接追加存储。
	// 5. 如果 leaderCommit>commitIndex，
	//    设置本地 commitIndex 为 leaderCommit 和最新日志索引中 较小的一个。
	// TODO: 2B
	// 2A: 仅考虑心跳包
	rf.mu.Lock()
	switch rf.serverState {
	case Follower:
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		} else {
			rf.followingLeader = true
			rf.currentTerm = args.Term
			// 重置等待时间
			rf.lastStartWaiting = time.Now().UnixNano() / 1e6
			//rf.printfLn("Reset time to %d", rf.lastStartWaiting)

			reply.Success = true
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		}
	case Candidate:
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		} else {
			rf.followingLeader = true
			rf.currentTerm = args.Term

			reply.Success = true
			reply.Term = rf.currentTerm
			rf.SwitchState(Follower)
			return
		}
	case Leader:
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		} else if args.Term == rf.currentTerm {
			DPrintf("Error! same currentTerm " + string(rune(rf.currentTerm)))
			rf.mu.Unlock()
			return
		} else {
			// 发现更新Leader/Server, 变为Follower
			rf.followingLeader = true
			rf.currentTerm = args.Term

			reply.Success = true
			reply.Term = rf.currentTerm
			rf.SwitchState(Follower)
			return
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.printfLn("AppendEntries %d->%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

/* RequestVotes */
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Receiver implementation:
	// 1. 如果 leader 的任期小于自己的任期返回 false。(5.1)
	// 2. 如果本地 voteFor 为空，候选者日志和本地日志相同， 则投票给该候选者 (5.2 和 5.4)
	rf.printfLn("Request Vote from %d in term %d", args.CandidateId, args.Term)
	rf.printfLn("Current Term %d", rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// TODO 2b, 候选者日志和本地日志相同比较
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.printfLn("%d Vote For %d", rf.me, args.CandidateId)
	} else {
		if rf.votedFor == -1 {
			// TODO 2b, 候选者日志和本地日志相同比较
			rf.votedFor = args.CandidateId
			rf.lastStartWaiting = time.Now().UnixNano() / 1e6

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
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

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) printfLn(format string, a ...interface{}) {
	fmt.Printf(fmt.Sprintf("[%d] [%s]_ ", rf.me, time.Unix(time.Now().Unix(), 0).Format("2006-01-02 15:04:05"))+format+"\n", a...)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.followingLeader = false
	rf.serverState = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]*LogEntry, 0, 10)

	rf.commitIndex = 0
	rf.lastApplied = 0

	// 先初始化再说
	rf.nextIndex = make([]int, 0, 10)
	rf.matchIndex = make([]int, 0, 10)

	rf.followerCounter = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动goroutine, 做状态转移
	rf.printfLn("Inited peer")
	rf.mu.Lock()
	rf.SwitchState(Follower)

	return rf
}
