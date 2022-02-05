package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nMap    int
	nReduce int

	// 0 - not start  1 - running  2 - finished  3 - failed
	mapTaskRecord []int

	// -1 - no corresponding task 0 - not start  1 - running  2 - finished  3 - failed
	reduceTaskRecord []int

	filenames []string

	mapisover bool

	reduceisover bool

	lock sync.Mutex

	condlock sync.Mutex
	cond     *sync.Cond

	done chan bool

	workerinfo map[int]*WorkerInfo

	iswatch bool
}

type WorkerInfo struct {
	recordlist map[int]bool
	starttime  time.Time
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// allocate map task for requesting worker
//
func (c *Coordinator) SendMapTask(args *AskForMapTaskArgs, reply *AskForMapTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	reply.O = true
	for i, v := range c.mapTaskRecord {
		if v == 0 || v == 3 {
			reply.F = c.filenames[i]
			reply.I = i
			reply.O = false
			c.mapTaskRecord[i] = 1
			break
		}
	}
	if reply.O {
		if args.S == 1 && c.mapTaskRecord[args.I] == 3 {
			// if all task has been over and task finished unnormally, allocate the original task
			reply.F = c.filenames[args.I]
			reply.I = args.I
			reply.O = false
			c.mapTaskRecord[args.I] = 1
		}
	}
	if !reply.O {
		_, ok := c.workerinfo[args.P]
		if ok {
			c.workerinfo[args.P].recordlist[reply.I] = true
			c.workerinfo[args.P].starttime = time.Now()
		} else {
			c.workerinfo[args.P] = &WorkerInfo{starttime: time.Now(), recordlist: make(map[int]bool)}
			c.workerinfo[args.P].recordlist[reply.I] = true
		}
	}
	reply.N = c.nReduce
	reply.D = c.mapisover
	return nil
}

//
// allocate reduce task for requesting worker
//
func (c *Coordinator) SendReduceTask(args *AskForReduceTaskArgs, reply *AskForReduceTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	reply.O = true
	for i, v := range c.reduceTaskRecord {
		if v == 0 || v == 3 {
			reply.I = i
			reply.O = false
			c.reduceTaskRecord[i] = 1
			fmt.Printf("send task %d\n", i)
			break
		}
	}
	if reply.O {
		if args.S == 1 && c.reduceTaskRecord[args.I] == 3 {
			// if all task has been over and task finished unnormally, allocate the original task
			reply.I = args.I
			reply.O = false
			c.reduceTaskRecord[args.I] = 1
		}
	}
	if !reply.O {
		_, ok := c.workerinfo[args.P]
		if ok {
			c.workerinfo[args.P].recordlist[reply.I] = true
			c.workerinfo[args.P].starttime = time.Now()
		} else {
			c.workerinfo[args.P] = &WorkerInfo{starttime: time.Now(), recordlist: make(map[int]bool)}
			c.workerinfo[args.P].recordlist[reply.I] = true
		}
	}
	reply.N = c.nReduce
	reply.M = c.nMap
	reply.D = c.reduceisover
	return nil
}

//
// Check Success map task and Set reduce route
//
func (c *Coordinator) CheckMapTask(args *CheckForMapTaskArgs, reply *CheckForMapTaskReply) error {
	// Set Map Task I success
	c.lock.Lock()
	c.mapTaskRecord[args.I] = 2
	for _, v := range args.R {
		c.reduceTaskRecord[v] = 0
	}
	c.lock.Unlock()
	flag := true
	c.lock.Lock()
	for _, v := range c.mapTaskRecord {
		if v != 2 {
			flag = false
			break
		}
	}
	fmt.Printf("CHECK-MAP %d success\n", args.I)
	c.lock.Unlock()
	if flag {
		c.lock.Lock()
		// clear the map
		for k := range c.workerinfo {
			delete(c.workerinfo, k)
		}
		c.mapisover = true
		c.lock.Unlock()

		c.cond.Broadcast()
		// fmt.Println("HERE")
		fmt.Println("ALL map task is over")
	}
	return nil
}

//
// Check Success reduce task and Set reduce route
//
func (c *Coordinator) CheckReduceTask(args *AskForReduceTaskArgs, reply *AskForReduceTaskReply) error {
	// Set Reduce Task I success
	c.lock.Lock()

	fmt.Printf("CHECK-Reduce %d check start\n", args.I)
	c.reduceTaskRecord[args.I] = 2
	flag := true
	for _, v := range c.reduceTaskRecord {
		if v != 2 && v != -1 {
			flag = false
			break
		}
	}
	if flag {
		c.reduceisover = true
		fmt.Printf("CHECK-Reduce %d over\n", args.I)
	}
	fmt.Printf("CHECK-Reduce %d success\n", args.I)
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) WatchWorkerAlive() {
	retry_time := 0
	for {
		select {
		case <-c.done:
			c.lock.Lock()
			c.iswatch = true
			c.lock.Unlock()
			fmt.Println("Watch thread over~")
			return
		default:
			fmt.Println("Watch all threads!!!")
			if c.reduceisover {
				return
			}
			// c.FindTimeoutWorker()
			if c.FindTimeoutWorker() {
				retry_time = 0
			} else {
				retry_time += 1
				if retry_time >= 10 {
					return
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func (c *Coordinator) FindTimeoutWorker() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	var res []int
	fmt.Println("Current workers are :")
	for k, v := range c.workerinfo {
		fmt.Println(k, time.Now().Sub(v.starttime))
		flag := false
		for i := range v.recordlist {
			if c.mapisover || c.mapTaskRecord[i] != 2 {
				flag = true
				break
			}
		}
		if flag && time.Now().Sub(v.starttime) >= time.Second*10 {
			// time out
			res = append(res, k)
		}
		// fmt.Print("\n")
	}
	// if c.mapisover {
	// 	fmt.Printf("left task int reduce phase:\n")
	// 	for i, v := range c.reduceTaskRecord {
	// 		fmt.Printf("%d(%d)   \n", i, v)
	// 	}
	// 	fmt.Print("\n")
	// } else {
	// 	fmt.Printf("left task int map phase:\n")
	// 	for i, v := range c.mapTaskRecord {
	// 		fmt.Printf("%d(%d): %s \n", i, v, c.filenames[i])
	// 	}
	// 	fmt.Print("\n")
	// }
	// fmt.Print("\n")
	for _, v := range res {
		for i := range c.workerinfo[v].recordlist {
			if c.mapisover {
				if c.reduceTaskRecord[i] != 2 {
					c.reduceTaskRecord[i] = 0
				}
			} else {
				fmt.Printf("File %s will rerun\n", c.filenames[i])
				c.mapTaskRecord[i] = 0
			}
		}
		delete(c.workerinfo, v)
	}
	fmt.Println("==================")
	return len(c.workerinfo) > 0
}

func (c *Coordinator) CheckReduceReady(args *AskForReduceReadyArgs, reply *AskForReduceReadyReply) error {

	judgeOver := func() bool {
		c.lock.Lock()
		defer c.lock.Unlock()
		return c.mapisover
	}

	for !judgeOver() {
		c.cond.L.Lock()
		c.cond.Wait()
		c.cond.L.Unlock()
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	// start watch
	go c.WatchWorkerAlive()
	// start listen
	go http.Serve(l, nil)
}

func (c *Coordinator) Construct(files []string, nReduce int) {
	c.cond = sync.NewCond(&c.condlock)
	c.mapisover = false
	c.reduceisover = false
	c.nMap = len(files)
	c.nReduce = nReduce
	c.filenames = files
	c.done = make(chan bool)
	c.workerinfo = make(map[int]*WorkerInfo)
	c.mapTaskRecord = make([]int, c.nMap)
	c.reduceTaskRecord = make([]int, c.nReduce)
	c.iswatch = true
	for i := 0; i < c.nMap; i++ {
		c.mapTaskRecord[i] = 0
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTaskRecord[i] = -1
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	// Your code here.
	if c.reduceisover {
		// remove mediate data
		c.done <- true
		fmt.Printf("CHECK-Reduce done\n")
		dir, _ := ioutil.ReadDir(mediate_dir)
		for _, d := range dir {
			os.RemoveAll(path.Join([]string{mediate_dir, d.Name()}...))
		}

		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.Construct(files, nReduce)
	c.server()
	return &c
}
