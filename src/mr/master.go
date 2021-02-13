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

type Task struct {
	currentState state
	identity     string
	mux          sync.Mutex
}

func (t *Task) CheckTaskIdle() bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	return t.currentState == idle
}

func (t *Task) CheckTaskCompleted() bool {
	t.mux.Lock()
	defer t.mux.Unlock()
	return t.currentState == completed
}

func (t *Task) SetTaskState(State state) {
	t.mux.Lock()
	t.currentState = State
	t.mux.Unlock()
}

func (t *Task) SetTaskIdle() {
	t.SetTaskState(idle)
}

func (t *Task) SetTaskInProgress() {
	t.SetTaskState(progress)
}

func (t *Task) SetTaskCompleted() {
	t.SetTaskState(completed)
}

func newTask() *Task {
	return &Task{currentState: idle, identity: ""}
}

type MapTask struct {
	*Task
	fileName string
	tasknum  int
}

type ReduceTask struct {
	*Task
	tasknum int
}

/* data structure for Master node and operations on it*/
type Master struct {
	// Your definitions here.
	nReduce     int
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
}

func (m *Master) allocateIdleTask() TaskType {
	allMapComplete := true
	for index, task := range m.MapTasks {
		if task.CheckTaskIdle() {
			allMapComplete = false
			task.SetTaskInProgress()
			return TaskType{"Map", task.fileName, index, m.nReduce}
		}

		if !task.CheckTaskCompleted() {
			allMapComplete = false
		}
	}

	allReduceComplete := true
	if allMapComplete {
		for index, task := range m.ReduceTasks {
			if task.CheckTaskIdle() {
				task.SetTaskInProgress()
				return TaskType{"Reduce", "", index, m.nReduce}
			}

			if !task.CheckTaskCompleted() {
				allReduceComplete = false
			}
		}
	}

	if allReduceComplete {
		return TaskType{"Done", "", -1, -1}
		// once the worker get the done task, it'll terminate itself
	}

	return TaskType{"None", "", -1, -1}
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

func (m *Master) GetTask(args *CallForWork, reply *TaskType) error {
	if args.Valid == true {
		*reply = m.allocateIdleTask()
		return nil
	}

	return errors.New("RPC INVALID.")

}

func (m *Master) CompleteTask(args *WorkComplete, reply *NilReply) error {
	if args.Tasktype == "Map" {
		if args.TaskNum < 0 || args.TaskNum > len(m.MapTasks) {
			fmt.Printf("Wrong index of map task :%v\n", args.TaskNum)
			return errors.New("Wrong index of map task.")
		}

		m.MapTasks[args.TaskNum].SetTaskCompleted()
		return nil
	}

	if args.Tasktype == "Reduce" {
		if args.TaskNum < 0 || args.TaskNum > len(m.ReduceTasks) {
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
	ret := true

	// Your code here.
	// TODO: if all the Map / Reduce Tasks are done, then return true
	for _, task := range m.ReduceTasks {
		if !task.CheckTaskCompleted() {
			ret = false
			break
		}
	}

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
	for index, fileName := range files {
		m.MapTasks = append(m.MapTasks, MapTask{newTask(), fileName, index})
	}

	for count := 0; count < nReduce; count++ {
		m.ReduceTasks = append(m.ReduceTasks, ReduceTask{newTask(), count})
	}

	m.server()
	return &m
}
