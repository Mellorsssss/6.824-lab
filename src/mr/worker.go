package mr

import (
	"fmt"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strings"
import "strconv"
import "encoding/json"
import "time"
import "regexp"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		taskType := CallTask()
		if taskType.Tasktype == "Map" {
			execMapApp(taskType, mapf)
			FinshTask("Map", taskType.TaskNum)
		} else if taskType.Tasktype == "Reduce" {
			execReduceApp(taskType, reducef)
			FinshTask("Reduce", taskType.TaskNum)
		} else if taskType.Tasktype == "Done" {
			fmt.Println("Work is done.Exit.")
			os.Exit(1)
		}
		time.Sleep(time.Second)
	}
}

func execMapApp(t TaskType, mapf func(string, string) []KeyValue) {
	file, err := os.Open(t.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", t.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", t.FileName)
	}
	file.Close()
	kva := mapf(t.FileName, string(content))

	reduceBuckets := make([][]KeyValue, t.Mod)
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % t.Mod
		reduceBuckets[reduceTaskNum] = append(reduceBuckets[reduceTaskNum], kv)
	}

	for index, kvarr := range reduceBuckets {
		file, err := os.Create(generateFileName(t.TaskNum, index))
		if err != nil {
			fmt.Printf("Create file failure: %s", generateFileName(t.TaskNum, index))
			continue
		}
		enc := json.NewEncoder(file)
		//fmt.Printf("Map Task Number: %v, Reduce Task Number: %v\n", t.TaskNum, index)
		for _, kv := range kvarr {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Encoding error in map phase.")
				continue
			}
		}
	}

}

func getReduceFiles(reduceTaskNum int) []string {
	pwd, _ := os.Getwd()
	fileInfoList, err := ioutil.ReadDir(pwd)
	if err != nil {
		log.Fatal(err)
		return []string{}
	}

	var validFileName = regexp.MustCompile(strings.Join([]string{`in-[0-9]+-`, strconv.Itoa(reduceTaskNum)}, ""))
	var res []string
	for i := range fileInfoList {
		if validFileName.MatchString(fileInfoList[i].Name()) {
			res = append(res, fileInfoList[i].Name())
		}
	}

	return res
}

func getIntermediate(reduceTaskNum int)[]KeyValue{
	fileNameList := getReduceFiles(reduceTaskNum)
	intermediate := []KeyValue{}
	for _, fileName := range fileNameList{
		file, err := os.Open(fileName)
		if err != nil{
			fmt.Printf("Can't open %v.\n", fileName)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	return intermediate
}

func execReduceApp(t TaskType, reducef func(string, []string) string) {
	intermediate := getIntermediate(t.TaskNum)
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"+strconv.Itoa(t.TaskNum)
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
}

func generateFileName(mapTaskNum int, reduceTaskNum int) string {
	return strings.Join([]string{"in", strconv.Itoa(mapTaskNum), strconv.Itoa(reduceTaskNum)}, "-")
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

func CallTask() TaskType {
	args := CallForWork{true}
	reply := TaskType{}
	call("Master.GetTask", &args, &reply)
	return reply
}

func FinshTask(tasktype string, tasknum int) {
	args := WorkComplete{tasktype, tasknum}
	reply := NilReply{}

	call("Master.CompleteTask", &args, &reply)
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
