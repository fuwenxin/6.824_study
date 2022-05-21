package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const mediate_dir = "mr-mediate/"

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
	MapTaskStart(mapf)

	ReduceTaskStart(reducef)
}

func ReadInputFromFile(filename string) (bool, string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		file.Close()
		return false, ""
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		file.Close()
		return false, ""
	}
	file.Close()
	return true, string(content)
}

func ReduceTaskStart(reducef func(string, []string) string) bool {
	CheckReadyToReduce()
	is_done := false
	index := -1
	retry_times := 0
	for !is_done {
		ok, reply := CallReduceTask(reducef, index)
		index = reply.I
		is_done = reply.D
		if is_done {
			break
		}
		if reply.O {
			time.Sleep(1 * time.Second)
			continue
		}
		if !ok {
			retry_times += 1
			if retry_times == 5 {
				log.Fatal("CallReduceTask error")
				break
			}
			continue
		}
		ok, mediate_data := ReadDataFromMediate(reply.M, reply.I)
		if !ok {
			log.Fatal("Read from mediate error")
			return false
		}
		sort.Sort(ByKey(mediate_data))

		err := RunReduceFunc(reducef, reply.I, mediate_data)
		if !err {
			log.Fatal("Run Reduce function error")
			return false
		}
		index = -1
	}
	return true
}

func RunReduceFunc(reducef func(string, []string) string, index int, data []KeyValue) bool {
	oname := "mr-out-" + strconv.Itoa(index)
	ofile, err := ioutil.TempFile(".", oname+"*")
	if err != nil {
		log.Fatal("Create reduce output file error")
		return false
	}
	for i := 0; i < len(data); {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := reducef(data[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", data[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(), oname)
	ofile.Close()
	SentReduceFinishMsg(index)
	fmt.Printf("Success input into %s\n", oname)
	return true
}

func ReadDataFromMediate(NMap int, index int) (bool, []KeyValue) {
	var mediate_data []KeyValue
	var mediate_files []string
	filepath.Walk(mediate_dir, func(filename string, fi os.FileInfo, err error) error {
		if fi.IsDir() {
			return nil
		}
		if strings.HasPrefix(filename, "mr-") && strings.HasSuffix(filename, strconv.Itoa(index)) {
			mediate_files = append(mediate_files, filename)
		}
		return nil
	})

	for _, mediate_file := range mediate_files {
		f, err := os.Open(mediate_file)

		if err != nil {
			fmt.Printf("Not exist : %s", mediate_file)
			f.Close()
			return false, nil
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			mediate_data = append(mediate_data, kv)
		}
		f.Close()
	}
	return true, mediate_data
}

func MapTaskStart(mapf func(string, string) []KeyValue) bool {
	var res bool
	var index int
	is_done := false
	index = -1
	retry_times := 0
	for !is_done {
		flag, reply := CallMapTask(mapf, index)
		index = reply.I
		is_done = reply.D
		if is_done {
			return true
		}
		if reply.O {
			time.Sleep(1 * time.Second)
			continue
		}
		if !flag {
			// error occur
			retry_times += 1
			if retry_times == 5 {
				log.Fatal("CallMapTask error")
				return false
			}
			continue
		}
		flag, content := ReadInputFromFile(reply.F)
		if !flag {
			// send failed msg
			log.Fatal("Read Frome file error ", reply.F)
			break
		} else {
			res = RunMapTaskFunc(mapf, reply, content)
			if res {
				index = -1 // mark the task finished successful
			} else {
				return false
			}
		}
	}
	return res
}

func RunMapTaskFunc(mapf func(string, string) []KeyValue, reply AskForMapTaskReply, content string) bool {
	key_file_map := make(map[string][]int)
	mediateData := mapf(reply.F, content)
	reduce_set := make(map[int]bool)
	index := reply.I
	for i, v := range mediateData {
		reduce_file_index := ihash(v.Key) % reply.N
		mediate_file_name := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(reduce_file_index)
		reduce_set[reduce_file_index] = true
		_, ok := key_file_map[mediate_file_name]
		if ok {
			key_file_map[mediate_file_name] = append(key_file_map[mediate_file_name], i)
		} else {
			key_file_map[mediate_file_name] = []int{i}
		}
	}

	if !CreateDir(mediate_dir) {
		return false
	}
	for k, v := range key_file_map {
		f, err := os.OpenFile(mediate_dir+k, os.O_CREATE|os.O_RDWR, os.ModePerm|os.ModeAppend)
		// fmt.Printf("Map write to %s\n", mediate_dir+k)
		if err != nil {
			fmt.Println(err)
			f.Close()
			return false
		}
		enc := json.NewEncoder(f)
		for _, index := range v {
			err := enc.Encode(&mediateData[index])
			if err != nil {
				fmt.Println(err)
				f.Close()
				return false
			}
		}
		f.Close()
	}
	res := SentMapFinishMsg(index, reduce_set)
	if res {
		fmt.Printf("map task success :%s %d", reply.F, os.Getpid())
	}
	return true
}

func CreateDir(dir string) bool {
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dir, os.ModePerm)
			if err != nil {
				fmt.Printf("Mkdir failed \n")
				return false
			}
		} else {
			fmt.Printf("stat file error \n")
			return false
		}
	}
	return true
}

func SentMapFinishMsg(index int, reduce_set map[int]bool) bool {
	args := CheckForMapTaskArgs{}
	args.I = index
	var reduces []int
	for k := range reduce_set {
		reduces = append(reduces, k)
	}
	args.R = reduces
	reply := CheckForMapTaskReply{}
	ok := call("Coordinator.CheckMapTask", &args, &reply)
	if !ok {
		log.Fatal("Map worker Send finish msg failed")
		return false
	}
	return true
}

func SentReduceFinishMsg(index int) {
	args := AskForReduceTaskArgs{}
	args.S = 2
	args.I = index
	reply := AskForReduceTaskReply{}
	ok := call("Coordinator.CheckReduceTask", &args, &reply)
	if !ok {
		log.Fatal("Reduce worker Send finish msg failed")
	}
}

func CallReduceTask(reducef func(string, []string) string, index int) (bool, AskForReduceTaskReply) {
	args := AskForReduceTaskArgs{}
	if index == -1 {
		args.S = 0
		args.I = -1
	} else {
		args.S = 1
		args.I = index
	}
	args.P = os.Getpid()
	reply := AskForReduceTaskReply{}
	ok := call("Coordinator.SendReduceTask", &args, &reply)
	if ok {
		// fmt.Printf("reduce index : %d \n", reply.I)
		return true, reply
	} else {
		fmt.Printf("call failed!\n")
		return false, reply
	}

}

func CallMapTask(mapf func(string, string) []KeyValue, index int) (bool, AskForMapTaskReply) {
	args := AskForMapTaskArgs{}
	if index == -1 {
		args.S = 0
		args.I = -1
	} else {
		args.S = 1
		args.I = index
	}
	args.P = os.Getpid()
	reply := AskForMapTaskReply{}
	fmt.Printf("Run Map task : %d\n ", args.P)

	ok := call("Coordinator.SendMapTask", &args, &reply)
	if ok {
		fmt.Printf("mapTask file : %v\n", reply.F)
		return true, reply
	} else {
		fmt.Printf("call failed!\n")
		return false, reply
	}
}

func CheckReadyToReduce() bool {
	args := AskForReduceReadyArgs{}
	reply := AskForReduceReadyReply{}

	ok := call("Coordinator.CheckReduceReady", &args, &reply)
	if ok {
		// fmt.Println("ready to Reduce")
		return true
	} else {
		fmt.Printf("Call failed\n")
		return false
	}
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
		fmt.Printf("reply.Y %v\n", reply.Y)
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
