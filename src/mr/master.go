package mr

import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	NumMap            int
	NumMapFinished    int
	NumReduce         int
	NumReduceFinished int
	mu                sync.Mutex

	MapTasks    []MapReduceTask
	ReduceTasks []MapReduceTask

	MapFinish    bool
	ReduceFinish bool
}

func (m *Master) checkTimeout(taskType string, num int, timeout int) {
	time.Sleep(time.Second * time.Duration(timeout))
	m.mu.Lock()
	defer m.mu.Unlock()
	if taskType == "Map" {
		if m.MapTasks[num].TaskStatus == "Assigned" {
			m.MapTasks[num].TaskStatus = "Unassigned"
		}
	} else {
		if m.ReduceTasks[num].TaskStatus == "Assigned" {
			m.ReduceTasks[num].TaskStatus = "Unassigned"
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) MapReduceHandler(args *MapReduceArgs, reply *MapReduceReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if args.MessageType == "request" {
		if !m.MapFinish {
			for index, task := range m.MapTasks {
				if task.TaskStatus == "Unassigned" {
					m.MapTasks[index].TaskStatus = "Assigned"
					reply.Task = m.MapTasks[index]
					go m.checkTimeout("Map", index, 20)
					return nil
				}
			}
			reply.Task.TaskType = "Wait"
			return nil
		} else if !m.ReduceFinish {
			for index, task := range m.ReduceTasks {
				if task.TaskStatus == "Unassigned" {
					m.ReduceTasks[index].TaskStatus = "Assigned"
					reply.Task = m.ReduceTasks[index]
					go m.checkTimeout("Reduce", index, 20)
					return nil
				}
			}
			reply.Task.TaskType = "Wait"
			return nil
		} else {
			return nil
		}
	} else if args.MessageType == "finish" {
		if args.Task.TaskType == "Map" {
			m.MapTasks[args.Task.TaskNum].TaskStatus = "Finished"
			m.NumMapFinished = m.NumMapFinished + 1
			if m.NumMapFinished == m.NumMap {
				m.MapFinish = true
			}
		} else {
			m.ReduceTasks[args.Task.TaskNum].TaskStatus = "Finished"
			m.NumReduceFinished = m.NumReduceFinished + 1
			if m.NumReduceFinished == m.NumReduce {
				m.ReduceFinish = true
			}
		}
		return nil
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.
	return m.ReduceFinish
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NumReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, NumReduce int) *Master {
	m := Master{}

	// Your code here.
	m.NumMap = len(files)
	m.NumReduce = NumReduce
	m.MapFinish = false
	m.ReduceFinish = false
	for index, file := range files {
		var tempTask MapReduceTask
		tempTask.NumMap = m.NumMap
		tempTask.NumReduce = m.NumReduce
		tempTask.TaskType = "Map"
		tempTask.TaskStatus = "Unassigned"
		tempTask.TaskNum = index
		tempTask.MapFile = file
		m.MapTasks = append(m.MapTasks, tempTask)
	}
	for i := 0; i < m.NumReduce; i++ {
		var tempTask MapReduceTask
		tempTask.NumMap = m.NumMap
		tempTask.NumReduce = m.NumReduce
		tempTask.TaskType = "Reduce"
		tempTask.TaskStatus = "Unassigned"
		tempTask.TaskNum = i
		for j := 0; j < m.NumMap; j++ {
			tempTask.ReduceFiles = append(tempTask.ReduceFiles, intermediateFilename(j, i))
		}
		m.ReduceTasks = append(m.ReduceTasks, tempTask)
	}

	m.server()
	return &m
}
