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
	"log"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

const electionTimeoutMinMs = 300
const electionTimeoutMaxMs = 600
const heartBeatTimeoutMs = 110
const updateCommitCycleMs = 20

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
	Command interface{}
}

// Server State
type ServerState int64

const (
	Follower  ServerState = 0
	Candidate             = 1
	Leader                = 2
)

func (ss *ServerState) ToString() string {
	switch *ss {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Fatal"
}

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

	applyChan chan ApplyMsg
}

func (rf *Raft) DebugInfo() {
	pLogs := make([]string, 0, 10)
	for _, v := range rf.log {
		pLogs = append(pLogs, fmt.Sprintf("%v:%v", v.TermId, v.Command))
	}
	rf.printfLn("Debug INFO: commitIndex-%v, nextIndex-%v, matchIndex-%v, log-%v",
		rf.commitIndex, rf.nextIndex, rf.matchIndex, pLogs)
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
		//rf.mu.Unlock()

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
							rf.mu.Unlock()
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
		//rf.mu.Unlock()
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
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log) - 1,
						LastLogTerm:  rf.log[len(rf.log)-1].TermId,
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
					rf.mu.Unlock()
					return
				}
				// 未超时，是否选够Majority
				if voteTerm > rf.currentTerm {
					// term更新，-> Follower
					rf.printfLn("Candidate -> Follower")
					rf.currentTerm = voteTerm
					rf.SwitchState(Follower)
					rf.mu.Unlock()
					return
				}
				rf.printfLn("with %d votes", voteCount)
				if voteCount > len(rf.peers)/2-1 {
					// 选够，自己成为Leader
					rf.printfLn("Win with %d votes", voteCount)
					rf.SwitchState(Leader)
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
			}
		}()
	case Leader:
		rf.serverState = Leader
		rf.printfLn("Become Leader")
		// 初始化leader的可变状态
		for i := 0; i < len(rf.peers); i++ {
			// nextIndex 每台机器在数组占据一个元素，元素的值为下条发送到该机器的日志索引
			// 初始值为 leader 最新一条日志的索引 +1
			rf.nextIndex[i] = len(rf.log)
			// 每台机器在数组中占据一个元素，元素的记录将要复制给该机器的最新日志 的索引
			// 初始值为 0
			rf.matchIndex[i] = 0
		}

		go func(currentTerm int) {
			// leader, 更新commitIndex
			for {
				rf.mu.Lock()
				if rf.killed() || rf.serverState != Leader || currentTerm != rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				maxCommitIndex := 0

				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					if rf.matchIndex[i] > maxCommitIndex {
						maxCommitIndex = rf.matchIndex[i]
					}
				}
				rf.printfLn("========== Check Commit [%d, %d]", rf.commitIndex+1, maxCommitIndex)
				for targetCommitIndex := rf.commitIndex + 1; targetCommitIndex <= maxCommitIndex; targetCommitIndex++ {
					// majority(matchIndex[i]>= N)（如果参与者大多数的 最新日志的索引大于 N）
					// 并且这些参与者索引为 N 的日志的任期也等于 leader 的当前任期
					// 更新commitIndex =N
					if targetCommitIndex >= len(rf.log) || rf.log[targetCommitIndex].TermId != rf.currentTerm {
						continue
					}
					cnt := 0
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}

						if rf.matchIndex[i] >= targetCommitIndex {
							cnt++
						}
					}

					if cnt > len(rf.peers)/2-1 {
						rf.printfLn("Update Committed to %d", targetCommitIndex)
						rf.commitIndex = targetCommitIndex
					}
				}

				rf.mu.Unlock()
				time.Sleep(updateCommitCycleMs * time.Millisecond)
			}
		}(rf.currentTerm)

		go func(currentTerm int) {
			// leader, 发送心跳
			for {
				rf.mu.Lock()
				if rf.killed() || rf.serverState != Leader || currentTerm != rf.currentTerm {
					rf.mu.Unlock()
					return
				}

				for index, _ := range rf.peers {
					if index == rf.me {
						continue
					}
					go AppendEntriesRoutine(rf, index)
				}
				rf.mu.Unlock()
				time.Sleep(heartBeatTimeoutMs * time.Millisecond)
			}
		}(rf.currentTerm)
		//rf.mu.Unlock()
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

func (aer *AppendEntriesReply) ToString() string {
	return fmt.Sprintf("AppendEntriesReply term=%d, success="+strconv.FormatBool(aer.Success), aer.Term)
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
	rf.mu.Lock()
	switch rf.serverState {
	case Follower:
		rf.printfLn("Follower Response...")
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		} else if args.Term == rf.currentTerm {
			rf.followingLeader = true
			rf.currentTerm = args.Term
			// 重置等待时间
			rf.lastStartWaiting = time.Now().UnixNano() / 1e6
			//rf.printfLn("Reset time to %d", rf.lastStartWaiting)

			reply.Success = true
			reply.Term = rf.currentTerm
			rf.printfLn("arg.PrevIndex=%d, rf.log.size=%d", args.PrevLogIndex, len(rf.log))
			if args.PrevLogIndex >= len(rf.log) {
				reply.Success = false
				rf.mu.Unlock()
				return
			} else if rf.log[args.PrevLogIndex].TermId != args.PrevLogTerm {
				reply.Success = false
				// delete the existing entry and all that follow it
				rf.log = rf.log[0:args.PrevLogIndex]
				rf.mu.Unlock()
				return
			}
			// Append any new entries not already in the log
			nextIndex := args.PrevLogIndex + 1
			for _, entry := range args.Entries {
				ee := entry
				if len(rf.log) >= nextIndex+1 {
					// 判断两边log是否相同，如果不等则需要覆盖
					// 偷懒: 直接覆盖算了
					rf.log[nextIndex] = &ee
					rf.printfLn("Cover To Index %v: %v", nextIndex, ee.Command)
					nextIndex++
					continue
				}
				rf.log = append(rf.log, &ee)
				rf.printfLn("Append To %d, Current Size = %d ==========", nextIndex, len(rf.log))
				nextIndex++
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = args.LeaderCommit
				if len(rf.log)-1 < rf.commitIndex {
					rf.commitIndex = len(rf.log) - 1
				}
			}
			rf.mu.Unlock()
			return
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm

			rf.followingLeader = true
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
	case Candidate:
		rf.printfLn("Candidate Response...")
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		} else {
			reply.Success = false
			reply.Term = rf.currentTerm - 1

			rf.followingLeader = true
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.SwitchState(Follower)
			rf.mu.Unlock()
			return
		}
	case Leader:
		rf.printfLn("Leader Response...")
		if args.Term < rf.currentTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			return
		} else if args.Term == rf.currentTerm {
			log.Fatalf("Error! same currentTerm " + string(rune(rf.currentTerm)))
			rf.mu.Unlock()
			return
		} else {
			// 发现更新Leader/Server, 变为Follower
			reply.Success = false
			reply.Term = rf.currentTerm

			rf.followingLeader = true
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.SwitchState(Follower)
			rf.mu.Unlock()
			return
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//rf.printfLn("AppendEntries %d->%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func AppendEntriesRoutine(rf *Raft, i int) (bool, *AppendEntriesReply) {
	rf.mu.Lock()
	if rf.serverState != Leader {
		rf.mu.Unlock()
		return false, nil
	}
	prevLogIndex := rf.nextIndex[i] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].TermId
	}

	rf.printfLn("AppendEntries to %d", i)
	// 复制需要传输的 entries
	toSendSize := len(rf.log) - rf.nextIndex[i]
	entries := make([]LogEntry, toSendSize, toSendSize)
	rf.printfLn("logSize=%d, rf.nextIndex[%d]=%d", len(rf.log), i, rf.nextIndex[i])
	for cnt := 0; cnt < toSendSize; cnt++ {
		idx := cnt + rf.nextIndex[i]
		entries[cnt] = *rf.log[idx]
	}

	arg := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, arg, reply)

	if !ok {
		return ok, reply
	}

	// 处理来自Follower的信息
	if reply.Success {
		rf.mu.Lock()
		newNextIndex := prevLogIndex + toSendSize + 1
		if newNextIndex > rf.nextIndex[i] {
			rf.nextIndex[i] = newNextIndex
			rf.matchIndex[i] = newNextIndex - 1
		}
		rf.printfLn("From %d, "+reply.ToString()+", prevLogIndex=%d", i, prevLogIndex)
		rf.mu.Unlock()
		return true, reply
	} else {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			// 转换为Follower
			rf.currentTerm = reply.Term
			rf.SwitchState(Follower)
			rf.printfLn("From %d, "+reply.ToString()+", prevLogIndex=%d", i, prevLogIndex)
			rf.mu.Unlock()
			return false, reply
		} else if reply.Term < rf.currentTerm {
			// retry, 不变nextIndex
			rf.printfLn("From %d, "+reply.ToString()+", prevLogIndex=%d", i, prevLogIndex)
			if rf.serverState != Leader {
				rf.mu.Unlock()
				return false, nil
			}
			rf.mu.Unlock()
			return AppendEntriesRoutine(rf, i)
		} else {
			// nextIndex --
			if rf.serverState != Leader {
				rf.mu.Unlock()
				return false, nil
			}
			rf.nextIndex[i]--
			rf.printfLn("From %d, "+reply.ToString()+", prevLogIndex=%d", i, prevLogIndex)
			rf.mu.Unlock()
			return AppendEntriesRoutine(rf, i)
		}
	}
	//return ok, reply
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
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
	} else if args.Term > rf.currentTerm {
		if rf.serverState != Follower {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.SwitchState(Follower)
		} else {
			rf.followingLeader = true
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastIndex := len(rf.log) - 1
			lastTermId := rf.log[lastIndex].TermId
			if lastTermId > args.LastLogTerm || (lastTermId == args.LastLogTerm && lastIndex > args.LastLogIndex) {
				// 拒绝投票
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
				rf.printfLn("%d Refuse Vote For %d", rf.me, args.CandidateId)
			} else {
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.printfLn("%d Vote For %d", rf.me, args.CandidateId)
			}
		} else {
			// 拒绝投票
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.printfLn("%d Refuse Vote For %d", rf.me, args.CandidateId)
		}
		rf.mu.Unlock()
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastIndex := len(rf.log) - 1
			lastTermId := rf.log[lastIndex].TermId
			if lastTermId > args.LastLogTerm || (lastTermId == args.LastLogTerm && lastIndex > args.LastLogIndex) {
				// 拒绝投票
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
				rf.printfLn("%d Refuse Vote For %d", rf.me, args.CandidateId)
			} else {
				rf.votedFor = args.CandidateId
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.printfLn("%d Vote For %d", rf.me, args.CandidateId)
			}
		} else {
			// 拒绝投票
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.printfLn("%d Refuse Vote For %d", rf.me, args.CandidateId)
		}
		rf.mu.Unlock()
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
	// Your code here (2B).
	rf.mu.Lock()
	if rf.serverState != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	// is leader, emit command
	// 追加日志到本地logs
	rf.printfLn("REQUEST TO Index: %d, Value:%d =======================", len(rf.log), command)
	rf.log = append(rf.log, &LogEntry{
		TermId:  rf.currentTerm,
		Command: command,
	})

	newIdx := len(rf.log) - 1
	newTermId := rf.currentTerm

	rf.mu.Unlock()

	// response to client, 写一个循环，检查commitId, 回应客户端...
	go func(logIndex int) {
		// 发起一轮 heartBeat
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			go func(i int) {
				ok := false
				for !ok {
					rf.mu.Lock()
					if rf.serverState != Leader {
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
					_ok, _ := AppendEntriesRoutine(rf, i)
					ok = _ok
					rf.printfLn("SEND Index %d =============================", len(rf.log)-1)
					// 重试
					if !ok {
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(idx)
		}
	}(newIdx)

	return newIdx, newTermId, true
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
	pLogs := make([]string, 0, 10)
	for _, v := range rf.log {
		pLogs = append(pLogs, fmt.Sprintf("%v:%v", v.TermId, v.Command))
	}
	di := fmt.Sprintf("_com-%v, nxt-%v, mat-%v, log-%v: ",
		rf.commitIndex, rf.nextIndex, rf.matchIndex, pLogs)
	fmt.Printf(
		fmt.Sprintf("[%d] [%s]_<term %d>"+di, rf.me, rf.serverState.ToString(), rf.currentTerm)+format+"\n",
		a...)
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
	rf.log = make([]*LogEntry, 1, 10)
	rf.log[0] = &LogEntry{
		TermId:  0,
		Command: "Dummy",
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	// 先初始化再说
	peerSize := len(rf.peers)
	rf.nextIndex = make([]int, peerSize, peerSize)
	rf.matchIndex = make([]int, peerSize, peerSize)

	rf.followerCounter = 0

	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动goroutine, 做状态转移
	rf.printfLn("Inited peer")

	// 启动goroutine, 如果 commitIndex > lastApplied：
	// 增加 lastApplied，并将日志 log[lastApplied] 应用到状态机
	go func() {
		// 循环check commit
		for {
			for {
				rf.mu.Lock()
				if rf.commitIndex > rf.lastApplied {
					rf.lastApplied++
					_command := rf.log[rf.lastApplied].Command
					_logIndex := rf.lastApplied
					rf.mu.Unlock()
					rf.printfLn("**** Try Apply Apply: %v: %v", _logIndex, _command)

					rf.applyChan <- ApplyMsg{
						CommandValid: true,
						Command:      _command,
						CommandIndex: _logIndex,
					}
					rf.printfLn("**** Apply: %v: %v", _logIndex, _command)

				} else {
					rf.mu.Unlock()
					break
				}
			}
			//rf.DebugInfo()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		time.Sleep(5 * time.Millisecond)
	}()

	rf.mu.Lock()
	rf.SwitchState(Follower)
	rf.mu.Unlock()

	return rf
}
