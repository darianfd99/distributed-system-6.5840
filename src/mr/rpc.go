package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

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

type TaskRequest struct{}

type MapTask struct {
	Id       int
	File     string
	Reducers int
}

type ReduceTask struct {
	Id    int
	Files []string
}

type FinishedTaskRequest struct {
	IsMap    bool
	Map      int
	IsReduce bool
	Reduce   int
}

type FinishedTaskResponse struct{}

type TaskReply struct {
	IsMap    bool
	Map      MapTask
	IsReduce bool
	Reduce   ReduceTask
	IsSleep  bool
	Sleep    time.Duration
	Done     bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
