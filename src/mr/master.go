package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "errors"
import "sync"
import "fmt"

/* data structure for Task type and operations on it*/
const (
	idle = iota
	progress
	completed
)

type state int

type Task struct{
	current_state state
	identity string
	mux      sync.Mutex 
}

func (t *Task) CheckTaskIdle() bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	return t.current_state == idle
}

func (t *Task) CheckTaskCompleted() bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	return t.current_state == completed
}

func (t *Task) SetTaskState(State state){
	t.mux.Lock()
	t.current_state = State
	t.mux.Unlock()
}

func (t *Task) SetTaskIdle(){
	t.SetTaskState(idle)
}

func (t *Task) SetTaskInProgress(){
	t.SetTaskState(progress)
}

func (t *Task) SetTaskCompleted(){
	t.SetTaskState(completed)
}

func newTask() *Task{
	return &Task{current_state:idle, identity:""}
}

type MapTask struct{
	*Task
	fileName string
	tasknum int
}

type ReduceTask struct{
	*Task
	fileName string
	tasknum int
}

/* data structure for Master node and operations on it*/
type Master struct {
	// Your definitions here.
	nReduce int
	MapTasks []MapTask
	ReduceTasks []ReduceTask

}

func (m *Master)allocateIdleTask() TaskType{
	for index, task := range m.MapTasks{
		if task.CheckTaskIdle(){
			task.SetTaskInProgress()
			return TaskType{"Map", task.fileName, index, m.nReduce}
		}
	} 

	for index, task := range m.ReduceTasks{
		if task.CheckTaskIdle(){
			task.SetTaskInProgress()
			return TaskType{"Reduce", task.fileName, index, m.nReduce}
		}
	}

	return TaskType{"Done", "", -1, -1} // once the worker get the done task, it'll terminate itself
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

func (m *Master) GetTask(args *CallForWork, reply *TaskType)error{
	if args.Valid == true{
		*reply = m.allocateIdleTask()
		return nil
	}
	
	return errors.New("RPC INVALID.")
	
}

func (m *Master) CompleteTask(args *WorkComplete, reply *NilReply )error {
	if args.Tasktype == "Map"{
		if args.TaskNum < 0 || args.TaskNum >len(m.MapTasks){
			fmt.Printf("Wrong index of map task :%v\n", args.TaskNum)
			return errors.New("Wrong index of map task.")
		}

		m.MapTasks[args.TaskNum].SetTaskCompleted()
		return nil
	}

	if args.Tasktype == "Reduce"{
		if args.TaskNum < 0 || args.TaskNum >len(m.ReduceTasks){
			fmt.Printf("Wrong index of reduce task :%v\n", args.TaskNum)
			return errors.New("Wrong index of reduce task.")
		}

		m.ReduceTasks[args.TaskNum].SetTaskCompleted()
		return nil
	}

	return errors.New("Unexpected type of task.")
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
	for index, fileName := range files{
		m.MapTasks = append(m.MapTasks, MapTask{newTask(), fileName, index})
	}

	m.server()
	return &m
}
