package mr

import (
	"log"
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
	serialMap map[int]SerialLog
	serialMux sync.Mutex
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
	success := args.Success
	//Serial := args.Serial
	filePaths := args.FilePaths
	taskType := args.TaskType
	if taskType == "map" {
		// TODO Map
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
		// TODO Map
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
	PrintLog("Register Work!")

	m.filesMux.Lock()
	if len(m.files) > 0 {
		// 分配到map
		toAllocateSize := m.filePerMap
		if len(m.files) < toAllocateSize {
			toAllocateSize = len(m.files)
		}
		allocateFiles := m.files[0:toAllocateSize]
		m.files = m.files[toAllocateSize:]
		m.filesMux.Unlock()

		// 返回一个map命令
		m.wmMux.Lock()
		m.wmStart = true
		reply.MapperNo = m.workingMap
		m.workingMap++
		m.wmMux.Unlock()
		reply.TaskType = "map"
		reply.FilePaths = allocateFiles
		reply.TotReduce = m.nReduce
		reply.Serial = m.NextSerial()
	} else {
		m.filesMux.Unlock()
		// map 已经全部分出去了，现在分reduce
		for true {
			m.wmMux.Lock()
			// mapping 是否已经结束?
			if !m.wmStart || m.workingMap > 0 {
				// mapping 未结束
				m.wmMux.Unlock()
				time.Sleep(time.Millisecond * 100)
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
	}
	PrintLog("Start Master...")
	m.server()
	return &m
}
