package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type SerialLog struct {
	sTime time.Time
	sType string // map | reduce

	workerDesc WorkerAskReply
}

type Master struct {
	// Your definitions here.
	// is done
	done    bool
	doneMux sync.Mutex
	// basic paras
	nReduce    int
	nMap       int
	filePerMap int
	// to mappers
	files    []string
	toMap    int
	filesMux sync.Mutex
	// from working mappers
	workingMap int
	wmStart    bool
	imFiles    []string
	wmMux      sync.Mutex
	// to reducers
	toReduce int
	reMux    sync.Mutex
	// from reducers
	wrMux       sync.Mutex
	finishedRed int
	// all mapper/reducer serial
	serial    int
	serialMux sync.Mutex

	serialMap map[int]SerialLog
	livingSet map[int]bool
	diedSet   map[int]bool
	logMux    sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) NextSerial() int {
	m.serialMux.Lock()
	toRet := m.serial
	m.serial++
	m.serialMux.Unlock()
	return toRet
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) WorkerFinish(args *WorkerReportArgs, reply *WorkerReportReply) error {
	m.logMux.Lock()
	if _, ok := m.livingSet[args.Serial]; !ok {
		// crashed, 忽略
		PrintLog("Ignore Crashed Return " + strconv.Itoa(args.Serial))
		m.logMux.Unlock()
		return nil
	}
	m.logMux.Unlock()

	success := args.Success
	//Serial := args.Serial
	filePaths := args.FilePaths
	taskType := args.TaskType
	if taskType == "map" {
		//MapperNo := args.MapperNo
		if !success {
			// TODO Mapper失败... 重试
			return nil
		}
		m.wmMux.Lock()
		m.workingMap--
		m.imFiles = append(m.imFiles, filePaths...)
		m.wmMux.Unlock()
	} else if taskType == "reduce" {
		//ReducerNo := args.ReducerNo
		if !success {
			// TODO Reducer失败... 重试
			return nil
		}
		m.finishedRed++
		if m.finishedRed == m.nReduce {
			m.doneMux.Lock()
			m.done = true
			m.doneMux.Unlock()
		}
	} else {
		// Do Nothing
	}

	return nil
}

func (m *Master) WorkerRegister(args *WorkerAskArgs, reply *WorkerAskReply) error {
	// 根据现在所需的mapper, reducer, 分配worker
	if m.done {
		reply.TaskType = "wait"
		return nil
	}
	PrintLog("Register Work! " + args.Name)

	// 失败的map/reduce 重启
	m.logMux.Lock()
	PrintLog("Try ReStart Task... " + args.Name)
	if len(m.diedSet) > 0 {
		PrintLog("To ReStart Task... " + args.Name)
		if reply.TaskType == "map" {
			PrintLog("Re-start crashed MAP {" + strconv.Itoa(reply.MapperNo) + "} ")
		} else if reply.TaskType == "reduce" {
			PrintLog("Re-start crashed REDUCE {" + strconv.Itoa(reply.ReducerNo) + "} ")
		}
		toRestart := -1
		for toRestart = range m.diedSet {
			*reply = m.serialMap[toRestart].workerDesc
			reply.Serial = m.NextSerial()
			m.livingSet[reply.Serial] = true
			m.serialMap[reply.Serial] = SerialLog{
				sTime:      time.Now(),
				sType:      reply.TaskType,
				workerDesc: *reply,
			}
			m.livingSet[reply.Serial] = true
			break
		}
		delete(m.diedSet, toRestart)
		m.logMux.Unlock()
		return nil
	}
	m.logMux.Unlock()

	PrintLog("No Restarted Task... " + args.Name)
	m.filesMux.Lock()

	if len(m.files) > 0 {
		// 分配到map
		PrintLog("Try Map... " + args.Name)
		toAllocateSize := m.filePerMap
		if len(m.files) < toAllocateSize {
			toAllocateSize = len(m.files)
		}
		allocateFiles := m.files[0:toAllocateSize]
		m.files = m.files[toAllocateSize:]
		reply.MapperNo = m.toMap
		m.toMap++
		m.filesMux.Unlock()

		// 返回一个map命令
		m.wmMux.Lock()
		m.wmStart = true
		m.workingMap++
		m.wmMux.Unlock()
		PrintLog("Master start Mapper " + strconv.Itoa(reply.MapperNo))
		reply.TaskType = "map"
		reply.FilePaths = allocateFiles
		reply.TotReduce = m.nReduce
		reply.Serial = m.NextSerial()

		//注册log
		m.logMux.Lock()
		m.serialMap[reply.Serial] = SerialLog{
			sTime:      time.Now(),
			sType:      "map",
			workerDesc: *reply,
		}
		m.livingSet[reply.Serial] = true
		m.logMux.Unlock()
	} else {
		PrintLog("Try Reduce... " + args.Name)
		m.filesMux.Unlock()
		// map 已经全部分出去了，现在分reduce
		for true {
			m.wmMux.Lock()
			// mapping 是否已经结束?
			if !m.wmStart || m.workingMap > 0 {
				// mapping 未结束
				m.wmMux.Unlock()
				reply.TaskType = "wait"
				break
			} else {
				// mapping 已结束
				m.wmMux.Unlock()
				m.reMux.Lock()
				// reduce 是否已经结束?
				if m.toReduce > 0 {
					// 需要 reducer
					reply.ReducerNo = m.nReduce - m.toReduce
					reply.TotReduce = m.nReduce
					m.toReduce--
					reply.TaskType = "reduce"
					reply.FilePaths = m.imFiles
					reply.Serial = m.NextSerial()
					m.reMux.Unlock()

					// 注册log
					m.logMux.Lock()
					m.serialMap[reply.Serial] = SerialLog{
						sTime:      time.Now(),
						sType:      "reduce",
						workerDesc: *reply,
					}
					m.livingSet[reply.Serial] = true
					m.logMux.Unlock()
				} else {
					// 不需要 reducer
					reply.TaskType = "wait"
					m.reMux.Unlock()
				}
				break
			}
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) monitor() {
	go func() {
		for true {
			m.logMux.Lock()
			t := time.Now()
			toDel := make(map[int]bool)
			for k := range m.livingSet {
				if t.Sub(m.serialMap[k].sTime) > 10*time.Second {
					typo := m.serialMap[k].workerDesc.TaskType
					if typo == "map" {
						PrintLog("Monitor: REMOVE Crashed MAP " + strconv.Itoa(m.serialMap[k].workerDesc.MapperNo))
					} else if typo == "reduce" {
						PrintLog("Monitor: REMOVE Crashed REDUCE " + strconv.Itoa(m.serialMap[k].workerDesc.ReducerNo))
					}
					m.diedSet[k] = true
					toDel[k] = true
				}
			}
			for k := range toDel {
				delete(m.livingSet, k)
			}
			PrintLog("Delete " + strconv.Itoa(len(toDel)) + " Tasks")
			m.logMux.Unlock()
			time.Sleep(time.Millisecond * 1000)
		}
	}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	if len(files) == 0 {
		m.done = true
	} else {
		m.serialMap = make(map[int]SerialLog)
		m.livingSet = make(map[int]bool)
		m.diedSet = make(map[int]bool)
		m.done = false
		m.nReduce = nReduce
		m.toReduce = nReduce
		m.nMap = nReduce * 4 // 最多几个mapper
		//m.nMap = 1
		m.filePerMap = len(files) / m.nMap
		m.files = files
		if m.nMap*m.filePerMap < len(files) {
			m.filePerMap += 1
		}
		PrintLog("MASTER file per map: " + strconv.Itoa(m.filePerMap))
	}
	PrintLog("Start Master...")
	m.monitor()
	m.server()
	return &m
}
