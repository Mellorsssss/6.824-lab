package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "errors"

const (
	idle = iota
	progress
	completed
)

type state int

type Task struct{
	current_state state
	identity string
}


func newTask() *Task{
	return &Task{idle, ""}
}

type MapTask struct{
	*Task
	fileName string
}

type ReduceTask struct{
	*Task
	fileName string
}

type Master struct {
	// Your definitions here.
	nReduce int
	MapTasks []MapTask
	ReduceTasks []ReduceTask

}

func (m *Master)findIdleTask() *TaskType{
	for _, task := range m.MapTasks{
		if task.current_state == idle{
			return &TaskType{"Map", task.fileName}
		}
	} 

	for _, task := range m.ReduceTasks{
		if task.current_state == idle{
			return &TaskType{"Reduce", task.fileName}
		}
	}
	return nil
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

func (m* Master) GetMapTask(args *CallForWork, reply *TaskType)error{
	if args.Valid == true{
		reply = m.findIdleTask()
		return nil
	}else{
		return errors.New("RPC INVALID.")
	}
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
	ret := false

	// Your code here.
	// TODO: if all the Map / Reduce Tasks are done, then return true
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	for _, fileName := range files{
		m.MapTasks = append(m.MapTasks, MapTask{newTask(), fileName})
	}

	m.server()
	return &m
}
