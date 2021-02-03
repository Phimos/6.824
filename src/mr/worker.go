package mr

import "fmt"
import "os"
import "log"
import "time"
import "sort"
import "io/ioutil"
import "encoding/json"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// map & reduce functions for this lab

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
func intermediateFilename(numMapTask int, numReduceTask int) string {
	return fmt.Sprintf("mr-%v-%v", numMapTask, numReduceTask)
}

func storeIntermediateFile(kva []KeyValue, filename string) {
	file, err := os.Create(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	enc := json.NewEncoder(file)
	if err != nil {
		log.Fatal("cannot create encoder")
	}
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("cannot encode")
		}
	}
}

func loadIntermediateFile(filename string) []KeyValue {
	var kva []KeyValue
	file, err := os.Open(filename)
	defer file.Close()

	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(file)
	for {
		kv := KeyValue{}
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

func finishTask(task MapReduceTask) {
	args := MapReduceArgs{MessageType: "finish", Task: task}
	reply := MapReduceReply{}

	call("Master.MapReduceHandler", &args, &reply)
}

func mapTask(mapf func(string, string) []KeyValue, task MapReduceTask) {
	filename := task.MapFile

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

	kvaa := make([][]KeyValue, task.NumReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NumReduce
		kvaa[idx] = append(kvaa[idx], kv)
	}

	for i := 0; i < task.NumReduce; i++ {
		storeIntermediateFile(kvaa[i], intermediateFilename(task.TaskNum, i))
	}

	defer finishTask(task)
}

func reduceTask(reducef func(string, []string) string, task MapReduceTask) {
	var intermediate []KeyValue
	for _, filename := range task.ReduceFiles {
		intermediate = append(intermediate, loadIntermediateFile(filename)...)
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", task.TaskNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

	defer finishTask(task)
}

func waitTask() {
	time.Sleep(time.Second)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := MapReduceArgs{MessageType: "request"}
		reply := MapReduceReply{}

		resp := call("Master.MapReduceHandler", &args, &reply)

		if !resp {
			break
		}

		switch reply.Task.TaskType {
		case "Map":
			mapTask(mapf, reply.Task)
		case "Reduce":
			reduceTask(reducef, reply.Task)
		case "Wait":
			waitTask()
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
