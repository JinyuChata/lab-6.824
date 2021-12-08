package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// # Worker ask for task
type WorkerAskArgs struct {
	Name string
}

type WorkerAskReply struct {
	TaskType  string   // map | reduce | wait
	FilePaths []string // map: [from_file_path]
	// reduce: [mapped_files, ...]
	ReducerNo int
	MapperNo  int

	TotReduce int
	Serial    int
}

// # Worker finish work
type WorkerReportArgs struct {
	Success  bool
	TaskType string
	Serial   int

	FilePaths []string
	// mapper
	MapperNo int
	// reducer
	ReducerNo int
}

type WorkerReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket Name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
