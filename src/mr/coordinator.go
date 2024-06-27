package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type TaskType uint8
type taskState uint8

const (
	mapper     TaskType  = 0
	reducer    TaskType  = 1
	exit       TaskType  = 2
	nowoker    taskState = 0
	executing  taskState = 1
	done       taskState = 2
	overtime   int64     = 3
	exitTaskId uint      = ^uint(0)
)

type worker struct {
	id      uint
	startat int64
}

type task struct {
	id       uint
	taskType TaskType
	files    []string
	mu       sync.Mutex
	workers  map[uint]worker
	state    taskState
}

// func (t *task) str() string {
// 	s := fmt.Sprintf("id:%v\ntaskType:%v\nfiles:%v\nworkers:%v\nstate:%v\n",
// 		t.id,
// 		t.taskType,
// 		t.files,
// 		t.workers,
// 		t.state)
// 	return s
// }

func newTask(id uint, files []string, taskType TaskType) *task {
	t := new(task)
	t.id = id
	t.taskType = taskType
	t.files = files
	t.workers = make(map[uint]worker, 0)
	t.state = nowoker
	return t
}

// asign a new worker to a task, the task should not be done
func (t *task) addNewWorker(id uint) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state == done {
		return errors.New("asign a worker to a completed task")
	}
	if len(t.workers) == 0 {
		t.state = executing
	}
	t.workers[id] = worker{id, time.Now().Unix()}
	return nil
}

// mark a task had been done
func (t *task) submit(id uint) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.workers[id]
	if !ok {
		return errors.New("Submit():worker not in this task")
	}
	if t.state != executing {
		return errors.New("resubmit")
	}

	t.state = done

	return nil
}

type timewheel struct {
	slots     []map[uint]bool
	cur       uint
	slotnum   uint
	c         *Coordinator
	task2Slot map[uint]uint
}

func (t *timewheel) construct(slotnum uint, c *Coordinator) {
	if slotnum == 0 {
		log.Fatalf("slotnum is zero")
	}

	t.slots = make([]map[uint]bool, slotnum)
	for i := 0; i < int(slotnum); i++ {
		t.slots[i] = make(map[uint]bool, 0)
	}
	t.cur = 0
	t.slotnum = slotnum
	t.c = c
	t.task2Slot = make(map[uint]uint, 0)
}

func (t *timewheel) add(taskid uint, coordinator *Coordinator, time int64) error {
	if time <= 0 || time > int64(t.slotnum) {
		return errors.New("time is zero")
	}
	sn := (t.cur + uint(time)) % t.slotnum
	t.slots[sn][taskid] = true

	// check
	_, ok := t.task2Slot[taskid]
	if ok {
		log.Fatalf("timer of task %v had been in timewheel", taskid)
	}

	t.task2Slot[taskid] = sn
	return nil
}

func (t *timewheel) cancel(taskid uint) {
	slot, ok := t.task2Slot[taskid]
	if !ok {
		return
	}
	delete(t.slots[slot], taskid)
	delete(t.task2Slot, taskid)
}

func (t *timewheel) tick() {
	taskids := []uint{}
	t.cur = (t.cur + 1) % t.slotnum
	for tid := range t.slots[t.cur] {
		taskids = append(taskids, tid)
		delete(t.task2Slot, tid)
	}

	if len(taskids) > 0 {
		t.c.enableTask(taskids)
		t.slots[t.cur] = make(map[uint]bool, 0)

		// fmt.Printf("overtime tasks:%v\n", taskids)
	}
}

type Coordinator struct {
	// max(nMap,nReduce)
	unDoneTasks chan uint
	nMap        uint
	nReduce     uint
	tasks       []*task
	// write channel per second
	ticker      *time.Ticker
	mu          sync.Mutex
	timer       *timewheel
	done        bool
	doneTaskNum uint
}

func (c *Coordinator) construct(files []string, nReduce int) {
	c.nMap = uint(len(files))
	c.nReduce = uint(nReduce)
	c.done = false
	c.doneTaskNum = 0
	if c.nMap > c.nReduce {
		c.unDoneTasks = make(chan uint, c.nMap)
	} else {
		c.unDoneTasks = make(chan uint, c.nReduce)
	}

	c.enableMap()

	c.tasks = make([]*task, c.nMap+c.nReduce)

	// each map task has one file
	for i := uint(0); i < c.nMap; i++ {
		c.tasks[i] = newTask(i, []string{files[i]}, mapper)
	}

	for i := uint(0); i < c.nReduce; i++ {
		reduceFiles := []string{}
		for j := uint(0); j < c.nMap; j++ {
			reduceFiles = append(reduceFiles, "mr-"+strconv.Itoa(int(j))+"-"+strconv.Itoa(int(i)))
		}

		slot, _ := c.getTaskSlot(reducer, i)
		c.tasks[slot] = newTask(i, reduceFiles, reducer)
	}

	c.ticker = time.NewTicker(time.Second * 1)

	c.timer = new(timewheel)
	c.timer.construct(uint(overtime), c)

	// fmt.Printf("unDoneTasks:%v(len=%v)\nnMap:%v\nnReduce:%v\ntimer:%v\ndone:%v\ndoneTaskNum:%v\n",
	// 	c.unDoneTasks,
	// 	len(c.unDoneTasks),
	// 	c.nMap,
	// 	c.nReduce,
	// 	c.timer,
	// 	c.done,
	// 	c.doneTaskNum)

	// for i := 0; i < len(c.tasks); i++ {
	// 	if c.tasks[i] == nil {
	// 		fmt.Printf("why task is nil?")
	// 	}
	// 	fmt.Printf("%v", c.tasks[i].str())
	// }
}

// add task to channel to add new woker to it
func (c *Coordinator) enableTask(tasks []uint) {
	for _, task := range tasks {
		c.unDoneTasks <- task
	}
}

func (c *Coordinator) enableReduce() {
	tasks := []uint{}
	var i uint = c.nMap
	var end uint = c.nMap + c.nReduce
	for i < end {
		tasks = append(tasks, i)
		i++
	}
	c.enableTask(tasks)
}

func (c *Coordinator) enableMap() {
	tasks := []uint{}
	for i := uint(0); i < c.nMap; i++ {
		tasks = append(tasks, i)
	}
	c.enableTask(tasks)
}

// add expired task to channel
func (c *Coordinator) close() {
	// close worker by pseudo task or close rpc
	// close coordinator by check c.done periodically, this work is done by mrcoordinator
	if c.done {
		log.Fatalf("asign task wrong, check logic of code")
	}

	// c.done = true
	// set clock to overtime, give some time for client to exit
	c.ticker.Stop()

	c.unDoneTasks <- exitTaskId

	c.done = true
}

func (c *Coordinator) GetTask(args *MapReduceArgs, reply *MapReduceReply) error {
	// may wait for a usable task. How to implement it?
	for {
		select {
		case <-c.ticker.C:
			c.mu.Lock()
			c.timer.tick()

			// if len(c.timer.task2Slot) > 0 {
			// 	fmt.Printf("timewheel tasks:%v\n", c.timer.task2Slot)
			// }

			c.mu.Unlock()
		case taskSlot := <-c.unDoneTasks:
			// fmt.Printf("get a new task, id=%v\n", taskSlot)
			if taskSlot == exitTaskId {
				// exit pseudo task
				reply.TaskType = exit
				reply.Taskid = exitTaskId

				// fill exit into channel
				c.unDoneTasks <- exitTaskId
				return nil
			}

			// set timer for task
			c.mu.Lock()
			if c.tasks[taskSlot].state == done {
				// fmt.Printf("task %v had been submited, ignore it\n", taskSlot)
				// the task had been completed in queue
				c.mu.Unlock()
				continue
			}
			err := c.timer.add(taskSlot, c, int64(overtime))
			if err != nil {
				log.Fatalf("add taskSlot %v failed", taskSlot)
			}
			c.mu.Unlock()

			// add new worker to task
			c.tasks[taskSlot].addNewWorker(args.Workerid)

			// set timer if possible
			reply.Files = c.tasks[taskSlot].files
			reply.TaskType = c.tasks[taskSlot].taskType
			if c.tasks[taskSlot].taskType == reducer {
				reply.Taskid = taskSlot - c.nMap
			} else {
				reply.Taskid = taskSlot
			}
			reply.NReduce = c.nReduce

			return nil
		}
	}
}
func (c *Coordinator) getTaskSlot(taskType TaskType, taskId uint) (uint, error) {
	if taskType != mapper && taskType != reducer {
		return 0, errors.New("unknown task type")
	}
	var taskSlot uint
	if taskType == mapper {
		taskSlot = taskId
	} else {
		taskSlot = taskId + c.nMap
	}

	if taskSlot >= c.nMap+c.nReduce {
		return 0, errors.New("invalid task id")
	}
	return taskSlot, nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	taskSlot, err := c.getTaskSlot(args.TaskType, args.Taskid)
	if err != nil {
		reply.State = 0
		return nil
	}

	err = c.tasks[taskSlot].submit(args.Workerid)
	if err != nil {
		// fmt.Printf("submit task %v failed, id=%v, type=%v\n", taskSlot, args.Taskid, args.TaskType)
		reply.State = 0
		// return error may cause client down
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.doneTaskNum = c.doneTaskNum + 1

	// fmt.Printf("submit task %v successful, id = %v, type = %v\n", taskSlot, args.Taskid, args.TaskType)

	if c.doneTaskNum == c.nMap {
		// fmt.Printf("all map task completed, ready to execute reduce task\n")
		c.enableReduce()
	} else if c.doneTaskNum == c.nMap+c.nReduce {
		// fmt.Printf("all task completed, ready to close\n")
		// all tasks have been completed, need to close worker and coordinator
		defer c.close()
	}

	// update timer
	c.timer.cancel(taskSlot)

	reply.State = 1

	return nil
}

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
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":12345")
	// sockname := coordinatorSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.construct(files, nReduce)

	c.server()
	return &c
}
