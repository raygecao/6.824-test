package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Work struct {
	sync.RWMutex
	iFiles []string
	// TODO use atomic to release mutex
	status Status
	done   chan struct{}
}

func (w *Work) safeUpdateStatus(status Status) {
	w.Lock()
	w.status = status
	w.Unlock()
}

type Stage int

const (
	MS Stage = iota
	RS
	FINISH
)

type Status int

const (
	Pending Status = iota
	Assigned
	Finished
)

type Master struct {
	// Your definitions here.

	sync.RWMutex
	stage       Stage
	nReduce     int
	mapWorks    []*Work
	reduceWorks []*Work
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AcquireWork(req *AcquireWorkRequest, resp *AcquireWorkResponse) error {
	monitorWork := func(w *Work) {
		select {
		case <-time.After(10 * time.Second):
			w.safeUpdateStatus(Pending)
		case <-w.done:
			w.safeUpdateStatus(Finished)
		}
	}

	m.RLock()
	stage := m.stage
	m.RUnlock()
	switch stage {
	case MS:
		var finished int
		for id, work := range m.mapWorks {
			work.Lock()
			switch work.status {
			case Finished:
				work.Unlock()
				finished++
				continue
			case Assigned:
				work.Unlock()
				continue
			}
			work.status = Assigned
			work.Unlock()
			*resp = AcquireWorkResponse{true, MAP, id, work.iFiles, m.nReduce}
			go monitorWork(work)
			fmt.Println(resp)
			return nil
		}
		fmt.Println("finished is ", finished)
		if finished == len(m.mapWorks) {
			m.Lock()
			m.stage = RS
			m.Unlock()
			reduceWorks := make([]*Work, m.nReduce)
			for i := 0; i < m.nReduce; i++ {
				files, err := filepath.Glob(fmt.Sprintf("mr-[0-9]*-%d", i))
				if err != nil {
					log.Fatal("fail to get intermedia files")
				}
				reduceWorks[i] = &Work{status: Pending, iFiles: files, done: make(chan struct{})}
			}
			m.reduceWorks = reduceWorks
			fmt.Println(m.reduceWorks)
		}
	case RS:
		var finished int
		for id, work := range m.reduceWorks {
			work := work
			work.Lock()
			switch work.status {
			case Finished:
				work.Unlock()
				finished++
				continue
			case Assigned:
				work.Unlock()
				continue
			}
			work.status = Assigned
			work.Unlock()

			*resp = AcquireWorkResponse{true, REDUCE, id, work.iFiles, m.nReduce}
			go monitorWork(work)
			return nil
		}
		if finished == m.nReduce {
			m.Lock()
			m.stage = FINISH
			m.Unlock()
		}
	case FINISH:
	}
	return nil
}

func (m *Master) FinishWork(req *FinishWorkRequest, resp *FinishWorkResponse) error {
	fmt.Println(req)
	var work *Work
	switch req.WorkType {
	case MAP:
		work = m.mapWorks[req.ID]
	case REDUCE:
		work = m.reduceWorks[req.ID]
	}
	work.RLock()
	status := work.status
	if status == Assigned {
		close(work.done)
	}
	work.RUnlock()
	fmt.Println("finish work")

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
	m.RLock()
	defer m.RUnlock()
	ret := m.stage == FINISH

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	// Your code here.
	// one file a map
	mapWorks := make([]*Work, len(files))
	for id, file := range files {
		fmt.Println(file)
		work := &Work{iFiles: []string{file}, done: make(chan struct{}), status: Pending}
		mapWorks[id] = work
	}
	m := Master{
		stage:    MS,
		nReduce:  nReduce,
		mapWorks: mapWorks,
	}

	m.server()
	return &m
}
