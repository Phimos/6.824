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
type MapReduceTask struct {
	TaskType   string // Map / Reduce / Wait
	TaskStatus string // Unassigned / Assigned / Finished
	TaskNum    int

	MapFile     string   // Input of Map task
	ReduceFiles []string // Input of Reduce task

	NumReduce int
	NumMap    int
}

// Args for RPC
type MapReduceArgs struct {
	MessageType string // request / finish
	Task        MapReduceTask
}

// Reply for RPC
type MapReduceReply struct {
	Task MapReduceTask
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
