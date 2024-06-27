package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type byKey []KeyValue

func (a byKey) Len() int               { return len(a) }
func (a byKey) Swap(i int, j int)      { a[i], a[j] = a[j], a[i] }
func (a byKey) Less(i int, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// read file and output each key-value to file named mr-mapperId-ihash(key) % bucketTotal
func execMap(mapf func(string, string) []KeyValue, filenames []string, mapperId uint, bucketTotal uint) error {
	intermediate := []KeyValue{}
	// generate key-value from files using mapf
	for _, filename := range filenames {
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
		intermediate = append(intermediate, kva...)
	}

	// create intermediate file, and divide key-value to each file using hash function
	files := make([]*os.File, bucketTotal)
	encs := make([]*json.Encoder, bucketTotal)
	iFileNames := make([]string, bucketTotal)
	var err error
	for i := uint(0); i < bucketTotal; i++ {
		iFileNames[i] = "mr-" + strconv.Itoa(int(mapperId)) + "-" + strconv.Itoa(int(i))
		files[i], err = os.CreateTemp(".", "temp-")
		if err != nil {
			return err
		}
		encs[i] = json.NewEncoder(files[i])
	}

	for _, kv := range intermediate {
		slot := ihash(kv.Key) % int(bucketTotal)
		err = encs[slot].Encode(&kv)
		if err != nil {
			return err
		}
	}

	// rename intermediate file to its actually name to avoid two Mapper using same file.
	// The coordiator may asign a task to two mapper
	for i := uint(0); i < bucketTotal; i++ {
		err = os.Rename(files[i].Name(), iFileNames[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// read content of input file, call reducef using these file as arg, then output result
func execReduce(reducef func(string, []string) string, filenames []string, reducerId uint) error {
	// read kev-value from file
	kvs := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}

		file.Close()
	}

	sort.Sort(byKey(kvs))

	oFileName := fmt.Sprintf("mr-out-%v", reducerId)

	oFile, err := os.CreateTemp(".", "temp-")
	if err != nil {
		return err
	}

	var i int = 0
	for i < len(kvs) {
		key := kvs[i].Key
		values := []string{kvs[i].Value}

		var j int = i + 1
		for j < len(kvs) {
			if kvs[i].Key != kvs[j].Key {
				break
			}
			values = append(values, kvs[j].Value)
			j++
		}

		i = j

		value := reducef(key, values)

		fmt.Fprintf(oFile, "%v %v\n", key, value)
	}

	os.Rename(oFile.Name(), oFileName)

	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	taskargs := MapReduceArgs{}
	taskargs.Workerid = uint(os.Getpid())

	for {
		taskreply := MapReduceReply{}
		// fmt.Printf("send task request\n")
		// fmt.Printf("%v\n", taskargs)
		ok := call("Coordinator.GetTask", &taskargs, &taskreply)
		if !ok {
			// fmt.Printf("GetTask failed\n")
			break
		}
		if taskreply.Taskid == exitTaskId || taskreply.TaskType == exit {
			// fmt.Printf("client: all task done, quit")
			break
		}

		// if taskreply.TaskType == reducer && taskreply.Taskid == 0 {
		// 	fmt.Printf("get a reduce task, id = 0\n")
		// }

		if taskreply.TaskType == mapper {
			execMap(mapf, taskreply.Files, taskreply.Taskid, taskreply.NReduce)
		} else if taskreply.TaskType == reducer {
			execReduce(reducef, taskreply.Files, taskreply.Taskid)
		} else {
			log.Fatalf("unknown task type")
		}

		// fmt.Printf("task request reply:\n")
		// fmt.Printf("%v\n", taskreply)

		submitarg := SubmitTaskArgs{}
		submitarg.Taskid = taskreply.Taskid
		submitarg.Workerid = taskargs.Workerid
		submitarg.TaskType = taskreply.TaskType

		submitreply := SubmitTaskReply{}

		// fmt.Printf("send submit request")
		// fmt.Printf("%v\n", submitarg)
		ok = call("Coordinator.SubmitTask", &submitarg, &submitreply)
		if !ok {
			break
		}
		// fmt.Printf("receive submit repley\n")
		// fmt.Printf("%v\n", submitreply)

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v, reply.\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":12345")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
