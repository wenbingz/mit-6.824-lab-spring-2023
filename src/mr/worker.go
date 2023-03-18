package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	RegisterFunc        = "Coordinator.Register"
	HeartBeat           = "Coordinator.HeartBeat"
	CompleteAMapTask    = "Coordinator.CompleteAMapTask"
	CompleteAReduceTask = "Coordinator.CompleteAReduceTask"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func register(workerInfo *WorkerInfo) {
	var workerId int
	call(RegisterFunc, workerInfo, &workerId)
	workerInfo.WorkerId = workerId
}

func executeAMapTask(workerInfo *WorkerInfo, mapTask *MapReduceTask, mapf func(string, string) []KeyValue) {
	filename := mapTask.Filename
	nReduce := mapTask.NReduce
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
	sort.Sort(ByKey(kva))
	for _, kv := range kva {
		key := kv.Key
		value := kv.Value
		reducerId := ihash(key) % nReduce
		outputFilename := fmt.Sprintf("mr-%d-%d", mapTask.Id, reducerId)
		f, err := os.OpenFile(outputFilename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatal("error when write output file ", err.Error())
		}
		if _, err := f.WriteString(key + " " + value + "\n"); err != nil {
			log.Fatal("error when write content into output file")
		}
		f.Close()
	}
}

func executeAReduceTask(workerInfo *WorkerInfo, reduceTask *MapReduceTask, reducef func(string, []string) string) {
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal("error when lsit candidate map output files ", err.Error())
	}
	for _, file := range files {
		name := file.Name()
		suffix := "-" + strconv.Itoa(reduceTask.Id)
		if !file.IsDir() && strings.HasSuffix(name, suffix) && strings.HasPrefix(name, "mr-") && !strings.HasPrefix(name, "mr-out") {
			f, err := os.Open(file.Name())
			if err != nil {
				log.Fatal("error when reading map output file " + name)
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			var preKey string = ""
			valueList := make([]string, 0, 10)
			output, err := os.OpenFile("mr-out-"+strconv.Itoa(reduceTask.Id), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			defer output.Close()
			for scanner.Scan() {
				line := scanner.Text()
				strs := strings.Split(line, " ")
				key, value := strs[0], strs[1]

				if preKey == "" {
					preKey = key
				} else if preKey == key {
					valueList = append(valueList, value)
				} else {
					// fmt.Println(preKey, valueList)
					content := reducef(preKey, valueList)
					output.WriteString(preKey + " " + content + "\n")
					preKey = key
					valueList = make([]string, 0, 10)
					valueList = append(valueList, value)
				}
			}
			if preKey != "" {
				content := reducef(preKey, valueList)
				output.WriteString(preKey + " " + content + "\n")
			}
		}
	}

}

func heartbeat(workInfo *WorkerInfo, resultTasks *[]MapReduceTask) {
	var result []MapReduceTask
	call(HeartBeat, workInfo, &result)
	if len(result) > 0 {
		// copy(resultTasks, result)
		for _, task := range result {
			*resultTasks = append(*resultTasks, task)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerInfo := &WorkerInfo{
		WorkerId: -1,
	}
	register(workerInfo)
	for {
		var tasks = make([]MapReduceTask, 0)
		heartbeat(workerInfo, &tasks)
		// fmt.Println("after heat", tasks)
		if len(tasks) > 0 {
			for _, task := range tasks {
				if task.IsMap {
					fmt.Printf("received a map task with id %d and filename %s \n", task.Id, task.Filename)
					executeAMapTask(workerInfo, &task, mapf)
					var reply int
					call(CompleteAMapTask, &task, &reply)
				} else {
					fmt.Printf("received a reduce task with id %d\n", task.Id)
					executeAReduceTask(workerInfo, &task, reducef)
					var reply int
					call(CompleteAReduceTask, &task, &reply)
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
