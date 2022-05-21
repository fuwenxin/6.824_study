package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

// Add your RPC definitions here.

type AskForMapTaskArgs struct {
	// 0 not-start 1-fail 2-finish
	S int
	// if fail , the failed index
	// if success, the successful index
	I int
	// current worker process's pid
	P int
}

type AskForMapTaskReply struct {
	// filename
	F string
	// the index of task in map tasks
	I int
	// the NReduce
	N int
	// whether over
	O bool
	// whether worker can be done
	D bool
}

type CheckForMapTaskArgs struct {
	// I map task success
	I int
	// the [Rth] reduce file is ready
	R []int
}

type CheckForMapTaskReply struct {
	O bool
}

type AskForReduceReadyArgs struct {
	O bool
}

type AskForReduceReadyReply struct {
	// whether ready
	N bool
}

type AskForReduceTaskArgs struct {
	// status : 0 - not start 1 - fail  2-finish
	S int
	// if fail , the failed index
	// if success, the successful index
	I int
	// current worker process's pid
	P int
}

type AskForReduceTaskReply struct {
	// the NMap
	M int
	// the index of task in map tasks
	I int
	// the NReduce
	N int
	// whether current worker's task over
	O bool
	// whether worker can be done
	D bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
