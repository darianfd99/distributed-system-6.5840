package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	AVAILABLE = iota
	IN_PROGRESS
	DONE
)

const (
	MAP = iota
	REDUCE
	JOB_DONE
)

type FileState uint8
type JobState uint8

type FileHandler struct {
	Filenames []string
	State     FileState
}

type Coordinator struct {
	mapFiles    []FileHandler
	reduceFiles []FileHandler
	Reducers    int
	jobState    JobState
	lock        *sync.RWMutex
}

func NewCoordinator(files []string, reducers int) Coordinator {
	mapFiles := make([]FileHandler, 0, len(files))
	reduceFiles := make([]FileHandler, 0, reducers)

	for _, file := range files {
		mapFiles = append(mapFiles, FileHandler{
			Filenames: []string{file},
			State:     AVAILABLE,
		})
	}

	for j := 0; j < reducers; j++ {
		map_out_files := make([]string, 0, len(files))
		for i := range files {
			map_out_files = append(map_out_files, fmt.Sprintf("mr-%d-%d", i, j))
		}

		reduceFiles = append(reduceFiles, FileHandler{
			Filenames: map_out_files,
			State:     AVAILABLE,
		})
	}

	return Coordinator{
		mapFiles:    mapFiles,
		reduceFiles: reduceFiles,
		Reducers:    reducers,
		jobState:    MAP,
		lock:        &sync.RWMutex{},
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

const sleepTime = 5 * time.Second

const taskTimeout = 10 * time.Second

func (c *Coordinator) assignMapTask(reply *TaskReply) error {
	for id, file := range c.mapFiles {
		if file.State == AVAILABLE {
			c.mapFiles[id].State = IN_PROGRESS

			*reply = TaskReply{
				IsMap: true,
				Map: MapTask{
					Id:       id,
					File:     file.Filenames[0],
					Reducers: c.Reducers,
				},
			}

			go func(id int, lock *sync.RWMutex) {
				time.Sleep(taskTimeout)
				if c.mapFiles[id].State == IN_PROGRESS {
					lock.Lock()
					c.mapFiles[id].State = AVAILABLE
					lock.Unlock()
				}
			}(id, c.lock)

			break
		}
	}
	c.lock.Unlock()

	if !reply.IsMap {
		*reply = TaskReply{
			IsSleep: true,
			Sleep:   sleepTime,
		}
	}

	return nil
}

func (c *Coordinator) assignReduceTask(reply *TaskReply) error {
	for id, file := range c.reduceFiles {
		if file.State == AVAILABLE {
			*reply = TaskReply{
				IsReduce: true,
				Reduce: ReduceTask{
					Id:    id,
					Files: file.Filenames,
				},
			}
			break
		}
	}
	if reply.IsReduce {
		c.reduceFiles[reply.Reduce.Id].State = IN_PROGRESS
		go func(id int, lock *sync.RWMutex) {
			time.Sleep(taskTimeout)
			if c.reduceFiles[id].State == IN_PROGRESS {
				lock.Lock()
				c.reduceFiles[id].State = AVAILABLE
				lock.Unlock()
			}
		}(reply.Reduce.Id, c.lock)
	}
	c.lock.Unlock()

	if !reply.IsReduce {
		*reply = TaskReply{
			IsSleep: true,
			Sleep:   sleepTime,
		}
		return nil
	}

	return nil
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {
	c.lock.Lock()
	state := c.jobState

	if state == MAP {
		return c.assignMapTask(reply)
	}

	if state == REDUCE {
		return c.assignReduceTask(reply)
	}

	*reply = TaskReply{
		Done: true,
	}

	return nil
}

func (c *Coordinator) check() {
	t := time.NewTicker(7 * time.Second)
	for range t.C {
		c.lock.RLock()
		state := c.jobState

		if state == MAP {
			c.checkMapStage()
			continue
		}

		if state == REDUCE {
			c.checkReduceStage()
			continue
		}

		if state == JOB_DONE {
			c.lock.RUnlock()
			return
		}

	}
}

func (c *Coordinator) checkMapStage() {
	allDone := true

	for _, file := range c.mapFiles {
		if file.State == AVAILABLE || file.State == IN_PROGRESS {
			allDone = false
			continue
		}
	}
	c.lock.RUnlock()
	if allDone {
		c.lock.Lock()
		c.jobState = REDUCE
		c.lock.Unlock()
	}
}

func (c *Coordinator) checkReduceStage() {
	allDone := true

	for _, file := range c.reduceFiles {
		if file.State == AVAILABLE || file.State == IN_PROGRESS {
			allDone = false
			continue
		}
	}
	c.lock.RUnlock()
	if allDone {
		c.lock.Lock()
		c.jobState = JOB_DONE
		c.lock.Unlock()
	}
}

func (c *Coordinator) FinishTask(args *FinishedTaskRequest, reply *FinishedTaskResponse) error {
	if args.IsMap {
		c.lock.Lock()
		c.mapFiles[args.Map].State = DONE
		c.lock.Unlock()
		return nil
	}

	if args.IsReduce {
		c.lock.Lock()
		c.reduceFiles[args.Reduce].State = DONE
		c.lock.Unlock()
		return nil
	}

	return fmt.Errorf("invalid finish task request")
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.RLock()
	done := c.jobState == JOB_DONE
	c.lock.RUnlock()
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := NewCoordinator(files, nReduce)
	go c.check()
	c.server()
	return &c
}
