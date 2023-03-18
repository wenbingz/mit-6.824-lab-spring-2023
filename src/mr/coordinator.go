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

type TaskStatus struct {
	IsCompleted bool
	IsSent      bool
	WorkerId    int
}

type WorkerInfo struct {
	WorkerId int
}

type Coordinator struct {
	// Your definitions here.
	MapTaskStatus       map[int]*TaskStatus
	ReduceTaskStatus    map[int]*TaskStatus
	MapTaskSet          map[int]*MapReduceTask
	ReduceTaskSet       map[int]*MapReduceTask
	CompletedMapTask    int
	CompletedReduceTask int
	WorkerMap           map[int]*WorkerInfo
	mutex               sync.Mutex
	WorkerCount         int
	Latches             *sync.WaitGroup
	WorkerQueue         map[int][]MapReduceTask
	Exit                bool
}

type MapReduceTask struct {
	Id       int
	IsMap    bool
	Filename string
	NReduce  int
}

func (c *Coordinator) CompleteAReduceTask(task *MapReduceTask, reply *int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.CompletedReduceTask++
	c.ReduceTaskStatus[task.Id].IsCompleted = true
	c.Latches.Done()
	return nil
}

func (c *Coordinator) CompleteAMapTask(task *MapReduceTask, reply *int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.CompletedMapTask++
	c.MapTaskStatus[task.Id].IsCompleted = true
	c.Latches.Done()
	return nil
}

func (c *Coordinator) sendATask(task *MapReduceTask, workerId int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.WorkerQueue[workerId] = append(c.WorkerQueue[workerId], *task)
	if task.IsMap {
		c.MapTaskStatus[task.Id] = &TaskStatus{
			IsCompleted: false,
			IsSent:      true,
			WorkerId:    workerId,
		}
	} else {
		c.ReduceTaskStatus[task.Id] = &TaskStatus{
			IsCompleted: false,
			IsSent:      true,
			WorkerId:    workerId,
		}
	}
	fmt.Printf("ready to assign task %d to worker %d\n", task.Id, workerId)
	return nil
}

func (c *Coordinator) Register(workerInfo *WorkerInfo, reply *int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	*reply = c.WorkerCount
	c.WorkerCount++
	workerInfo.WorkerId = *reply
	c.WorkerMap[workerInfo.WorkerId] = workerInfo
	return nil
}

func (c *Coordinator) HeartBeat(info *WorkerInfo, tasks *[]MapReduceTask) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// fmt.Println("received a heartbeat")
	ts, ok := c.WorkerQueue[info.WorkerId]
	if ok && len(ts) > 0 {
		// fmt.Printf("send task with length %d to worker %d\n", len(ts), info.WorkerId)
		*tasks = ts
		c.WorkerQueue[info.WorkerId] = make([]MapReduceTask, 0)
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	return c.Exit
}

func (c *Coordinator) start(files []string, nReduce int) {
	time.Sleep(10 * time.Second)
	for idx, file := range files {
		c.MapTaskSet[idx] = &MapReduceTask{
			Id:       idx,
			Filename: file,
			IsMap:    true,
			NReduce:  nReduce,
		}
	}
	c.Latches = new(sync.WaitGroup)
	c.Latches.Add(len(c.MapTaskSet))
	for k, v := range c.MapTaskSet {
		c.sendATask(v, k%c.WorkerCount)
	}
	// waiting all the map tasks to complete
	c.Latches.Wait()
	c.Latches.Add(nReduce)
	for i := 0; i < nReduce; i++ {
		c.sendATask(&MapReduceTask{Id: i, IsMap: false}, i%c.WorkerCount)
	}
	c.Latches.Wait()
	c.Exit = true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTaskStatus:       make(map[int]*TaskStatus),
		ReduceTaskStatus:    make(map[int]*TaskStatus),
		MapTaskSet:          make(map[int]*MapReduceTask),
		ReduceTaskSet:       make(map[int]*MapReduceTask),
		CompletedReduceTask: 0,
		CompletedMapTask:    0,
		WorkerMap:           make(map[int]*WorkerInfo),
		WorkerCount:         0,
		Exit:                false,
		WorkerQueue:         make(map[int][]MapReduceTask),
		mutex:               sync.Mutex{},
	}
	c.server()
	// waiting workers to join in
	go c.start(files, nReduce)
	return &c
}
