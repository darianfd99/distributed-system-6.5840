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
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type File struct {
	filename string
	fd       *os.File
}

func execMap(m *MapTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(m.File)
	if err != nil {
		log.Fatalf("cannot open %v", m.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", m.File)
	}
	file.Close()

	fmt.Println("EXECUTING MAP")
	kvs := mapf(m.File, string(content))

	files := make([]File, 0, m.Reducers)
	for j := 0; j < m.Reducers; j++ {
		filename := fmt.Sprintf("mr-%d-%d-tmp", m.Id, j)
		ofile, err := os.CreateTemp(".", filename)
		if err != nil {
			log.Fatalf("can not create: %v", ofile)
		}
		defer ofile.Close()
		files = append(files, File{
			filename: filename,
			fd:       ofile,
		})
	}

	for _, kv := range kvs {
		hash := ihash(kv.Key) % m.Reducers
		fmt.Fprintf(files[hash].fd, "%v\t%v\n", kv.Key, kv.Value)
	}

	for _, f := range files {
		path := strings.Split(f.fd.Name(), "-tmp")[0]
		err := os.Rename(f.fd.Name(), path)
		if err != nil {
			log.Fatal(err)
		}
	}

}

func execReduce(m *ReduceTask, reducef func(string, []string) string) {
	fmt.Println("EXECUTING REDUCE")
	kvs := []KeyValue{}
	for _, filename := range m.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.Split(scanner.Text(), "\t")
			kvs = append(kvs, KeyValue{
				Key:   line[0],
				Value: line[1],
			})
		}
	}

	sort.Sort(ByKey(kvs))

	oname := fmt.Sprintf("mr-out-%d", m.Id)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}

		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task, err := CallAssignTask()
		if err != nil {
			break
		}

		if task.Done {
			break
		}

		if task.IsSleep {
			time.Sleep(task.Sleep)
			continue
		}

		if task.IsMap {
			execMap(&task.Map, mapf)
			CallFinishTask(FinishedTaskRequest{
				IsMap: true,
				Map:   task.Map.Id,
			})
			continue
		}

		if task.IsReduce {
			execReduce(&task.Reduce, reducef)
			CallFinishTask(FinishedTaskRequest{
				IsReduce: true,
				Reduce:   task.Reduce.Id,
			})
			continue
		}

		log.Fatalf("invalid task was received")
	}
}

func CallAssignTask() (*TaskReply, error) {
	args := TaskRequest{}
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		return nil, fmt.Errorf("call failed")
	}

	return &reply, nil
}

func CallFinishTask(req FinishedTaskRequest) (*FinishedTaskResponse, error) {
	reply := FinishedTaskResponse{}

	ok := call("Coordinator.FinishTask", &req, &reply)
	if !ok {
		return nil, fmt.Errorf("call failed")
	}

	return &reply, nil
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
