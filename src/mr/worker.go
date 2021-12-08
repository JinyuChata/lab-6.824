package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	PrintLog("Start Worker...")
	for true {
		registerReply, succ := CallRegister()
		if !succ {
			PrintLog("Worker Exit")
			break
		}
		serial := registerReply.Serial

		if registerReply.TaskType == "map" {
			mapperNo := registerReply.MapperNo
			nReduce := registerReply.TotReduce
			intermediate := []KeyValue{}
			PrintLog("Start Map " + strconv.Itoa(mapperNo))
			for _, filename := range registerReply.FilePaths {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}
			// write intermediate to intermediate_path
			m := make(map[int][]KeyValue)
			for _, v := range intermediate {
				targetReducer := ihash(v.Key) % nReduce
				if _, ok := m[targetReducer]; ok {
					m[targetReducer] = append(m[targetReducer], v)
				} else {
					m[targetReducer] = []KeyValue{v}
				}
			}

			toSend := []string{}
			for k, v := range m {
				fname := fmt.Sprintf("mr-%v-%v", mapperNo, k)
				toSend = append(toSend, fname)
				file, _ := os.Create(fname)
				enc := json.NewEncoder(file)
				err := enc.Encode(v)
				if err != nil {
					log.Fatalf("Error encode intermediate file")
				}
			}
			w_arg := WorkerReportArgs{Success: true, TaskType: "map", Serial: serial,
				FilePaths: toSend, MapperNo: mapperNo, ReducerNo: 0}
			CallResult(w_arg)
		} else if registerReply.TaskType == "reduce" {
			intermediate := []KeyValue{}
			reducerNo := registerReply.ReducerNo
			PrintLog("Start Reduce " + strconv.Itoa(reducerNo))
			for _, filename := range registerReply.FilePaths {
				if !strings.HasSuffix(filename, fmt.Sprintf("-%v", reducerNo)) {
					continue
				}
				// reduce...
				file, _ := os.Open(filename)
				dec := json.NewDecoder(file)
				var new_intermediate []KeyValue
				if err := dec.Decode(&new_intermediate); err != nil {
					break
				}
				intermediate = append(intermediate, new_intermediate...)
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", reducerNo)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()
			// TODO send intermediate_path back to master (only 1)
			w_arg := WorkerReportArgs{Success: true, TaskType: "reduce", Serial: serial,
				FilePaths: []string{oname}, MapperNo: 0, ReducerNo: reducerNo}
			if _, s := CallResult(w_arg); !s {
				PrintLog("Worker Exit")
				break
			}
		} else {
			// wait 300ms
			time.Sleep(time.Millisecond * 100)
		}
	}
}

// call register
func CallRegister() (WorkerAskReply, bool) {
	args := WorkerAskArgs{}
	reply := WorkerAskReply{}
	succ := call("Master.WorkerRegister", &args, &reply)
	return reply, succ
}

// call result
func CallResult(args WorkerReportArgs) (WorkerReportReply, bool) {
	reply := WorkerReportReply{}
	succ := call("Master.WorkerFinish", &args, &reply)
	return reply, succ
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
		//log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
