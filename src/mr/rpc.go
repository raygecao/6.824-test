package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
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

type WorkType int

const (
	MAP WorkType = iota
	REDUCE
)

// Add your RPC definitions here.
type AcquireWorkRequest struct {
}

type AcquireWorkResponse struct {
	// Whether assigned a task, if false, the works are all assigned
	// It should be query in loop since some task may be failed
	Acquired bool
	// Whether it got a map work or a reduce work
	WorkType WorkType
	// The work index
	ID int
	// The files for processed
	Files   []string
	NReduce int
}

type FinishWorkRequest struct {
	WorkType WorkType
	ID       int
}

type FinishWorkResponse struct {
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
