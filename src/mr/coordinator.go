package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type MrTaskType int
const (
	InvalidWorkerId = -1
	InvalidMapId = -1
	InvalidRedId = -1
	InvalidTaskId = -1
)
const (
	MapTask MrTaskType = iota
	RedTask
)
// shared by Coordinator and Worker
type TaskInfo struct {
	TaskId int
	TaskType MrTaskType
	NReduce int
	Files []string
	MapId int
	RedId int
}
type ProcessResultType int
const (
	ProcessFailed ProcessResultType = iota
	ProcessSucess
)

// only used by Coordinator
type coTaskStat int
const (
	coIdle coTaskStat = iota
	coRunning
	coCompleted
)
type coTaskInfo struct {
	BaseInfo TaskInfo
	Stat coTaskStat
	DealingWorker int
	StartTime time.Time
}
type coTask interface {
	GetTaskInfo() TaskInfo
	OutOfTime() bool
	SetNow()
	SetWorker(id int)
	SetStat(stat coTaskStat)
	GetWorkerId() int
	GetStat() coTaskStat
}
type coMapTask struct {
	Info coTaskInfo
}
type coRedTask struct {
	Info coTaskInfo
}
func (c *coTaskInfo) GetTaskInfo() TaskInfo {
	return c.BaseInfo
}
func (c *coTaskInfo) OutOfTime() bool {
	return c.Stat == coRunning && time.Since(c.StartTime) >= 10 * time.Second
}
func (c *coTaskInfo) SetNow() {
	c.StartTime = time.Now()
}
func (c *coTaskInfo) SetWorker(id int) {
	c.DealingWorker = id
}
func (c *coTaskInfo) SetStat(stat coTaskStat) {
	c.Stat = stat
}
func (c *coTaskInfo) GetWorkerId() int {
	return c.DealingWorker
}
func (c *coTaskInfo) GetStat() coTaskStat {
	return c.Stat
}

type coTaskSet map[*coTaskInfo]struct{}
func (s *coTaskSet) Size() int {
	return len(*s)
}
func (s *coTaskSet) Empty() bool {
	return s.Size() == 0
}
func (s *coTaskSet) PopTask() *coTaskInfo {
	if s.Empty() {
		panic("Getting task from an empty set.")
	}
	for task := range *s {
		s.Erase(task)
		return task
	}
	return nil
}
func (s *coTaskSet) Contains(task *coTaskInfo) bool {
	_, ok := (*s)[task]
	return ok
}
func (s *coTaskSet) Erase(task *coTaskInfo) {
	if !s.Contains(task) {
		panic("Erasing task that not in the set.")
	}
	delete(*s, task)
}
func (s *coTaskSet) Insert(task *coTaskInfo) {
	if s.Contains(task) {
		return
	}
	(*s)[task] = struct{}{}
}


// used by Coordinator and Worker
type WorkStat int
const (
	Died WorkStat = iota
	Alive
	MapPhase
	RedPhase
	AllDone
)
type WorkerInfo struct {
	Id int
}
// used by Coordinator
type coWorkerInfo struct {
	BaseInfo WorkerInfo
	Stat WorkStat
	LastHeartTime time.Time
	DealingTasks coTaskSet
}
type coWorker interface {
	GetWorkerInfo() WorkerInfo
	GetWorkerStat() WorkStat
	UpdateHeartTime()
	OutOfTime() bool
	GetDealingTasks() coTaskSet
}
func (w *coWorkerInfo) GetWorkerInfo() WorkerInfo {
	return w.BaseInfo
}
func (w *coWorkerInfo) GetWorkerStat() WorkStat {
	return w.Stat
}
func (w *coWorkerInfo) UpdateHeartTime() {
	w.LastHeartTime = time.Now()
}
func (w *coWorkerInfo) OutOfTime() bool {
	return time.Since(w.LastHeartTime) >= 10 * time.Second
}
func (w *coWorkerInfo) GetDealingTasks() coTaskSet {
	return w.DealingTasks
}

type Coordinator struct {
	// Your definitions here.
	nReduce int
	nextMapTaskId int
	nextWorkerId int
	idleMapTasks coTaskSet
	runningMapTasks coTaskSet
	idleRedTasks coTaskSet
	runningRedTasks coTaskSet
	workers map[int]*coWorkerInfo
	idleWorkerId map[int]struct{}
	mtx sync.Mutex
	nextTaskId int
	curPhase WorkStat
	tasks map[int]*coTaskInfo
	redFiles map[int][]string
}

func (c *Coordinator) GenerateMapTask(file string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	base := TaskInfo {
		TaskId: c.nextTaskId,
		TaskType: MapTask,
		NReduce: c.nReduce,
		Files: []string{file},
		MapId: c.nextMapTaskId,
		RedId: InvalidRedId,
	}
	c.nextTaskId++
	c.nextMapTaskId++
	task := coTaskInfo {
		BaseInfo: base,
		Stat: coIdle,
		DealingWorker: InvalidWorkerId,
		StartTime: time.Now(),
	}
	c.idleMapTasks.Insert(&task)
	c.tasks[base.TaskId] = &task
	log.Printf("Coordinator: inserted a new map task: \n%+v\n", task)
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RegisterWorker(req *RegisterWorkerReq, rsp *RegisterWorkerRsp) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	worker := WorkerInfo {
		Id: InvalidWorkerId,
	}
	if len(c.idleWorkerId) != 0 {
		for id := range c.idleWorkerId {
			delete(c.idleWorkerId, id)
			worker.Id = id
			break
		}
	} else {
		worker.Id = c.nextWorkerId
		c.nextWorkerId++
	}
	rsp.Info = worker
	c.workers[worker.Id] = &coWorkerInfo{
		BaseInfo: worker,
		Stat: Alive,
		LastHeartTime: time.Now(),
		DealingTasks: coTaskSet{},
	}
	log.Printf("Coordinator: registered worker %d.\n", worker.Id)
	return nil
}

func (c *Coordinator) AskForTask(req *AskForTaskReq, rsp *AskForTaskRsp) error {
	id := req.Worker.Id
	log.Printf("worker %d is asking for a task.\n", id)
	c.mtx.Lock()
	defer c.mtx.Unlock()
	worker, ok := c.workers[id]
	if !ok {
		log.Printf("worker %d is an invalid worker, rejected.\n", id)
		rsp.Task.TaskId = InvalidTaskId
		return nil
	}
	if c.curPhase == AllDone || c.curPhase == MapPhase && c.idleMapTasks.Empty() ||
		c.curPhase == RedPhase && c.idleRedTasks.Empty() {
		rsp.Task.TaskId = InvalidTaskId
		log.Printf("no available tasks for worker %d, rejected.\n", id)
		return nil
	}
	var task *coTaskInfo = nil
	if c.curPhase == MapPhase {
		task = c.idleMapTasks.PopTask()
		c.runningMapTasks.Insert(task)
	} else {
		// RedPhase
		task = c.idleRedTasks.PopTask()
		c.runningRedTasks.Insert(task)
	}
	task.Stat = coRunning
	task.DealingWorker = id
	task.StartTime = time.Now()
	rsp.Task = task.BaseInfo
	worker.DealingTasks.Insert(task)
	log.Printf("assigned worker %d a task:\n%+v\n", id, *task)
	go func(id int, task *coTaskInfo, startTime time.Time) {
		time.Sleep(15 * time.Second)
		c.mtx.Lock()
		defer c.mtx.Unlock()
		if task.DealingWorker == id && task.OutOfTime() && task.StartTime == startTime {
			log.Printf("worker %d processed task out of time. Task:%+v\n", id, *task)
			task.DealingWorker = InvalidWorkerId
			task.Stat = coIdle
			c.workers[id].DealingTasks.Erase(task)
			if c.curPhase == MapPhase {
				c.runningMapTasks.Erase(task)
				c.idleMapTasks.Insert(task)
			} else {
				c.runningRedTasks.Erase(task)
				c.idleRedTasks.Insert(task)
			}
			log.Printf("turned task to idle. Task:\n%+v", *task)
		}
	}(id, task, task.StartTime)
	return nil
}

func (c *Coordinator) ReportTask(req *ReportTaskReq, rsp *ReportTaskRsp) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	id := req.Worker.Id
	task := req.Task
	result := req.Result

	worker, ok := c.workers[id]
	if !ok {
		log.Printf("received invalid worker %d task report. skipped.\nreq:%+v\n", id, req)
		return nil
	}
	coTask, ok := c.tasks[task.TaskId]
	if !ok || coTask.DealingWorker != id || coTask.Stat != coRunning || !worker.DealingTasks.Contains(coTask) {
		log.Printf("The task doesnot exist or the worker isnot the owner of the task.\n")
		return nil
	}

	worker.DealingTasks.Erase(coTask)
	coTask.DealingWorker = InvalidWorkerId
	taskType := coTask.BaseInfo.TaskType
	if result == ProcessSucess {
		coTask.Stat = coCompleted
		log.Printf("worker %d process task:\n%+v\n sucessfully.\n", id, req.Task)
	} else {
		coTask.Stat = coIdle
		log.Printf("worker %d process task:\n%+v\n failed.\n The Task will be rerun.\n", id, *coTask)
	}
	if taskType == MapTask {
		c.runningMapTasks.Erase(coTask)
		if result == ProcessSucess {
			// collect map intermediate files...
			for i := 0; i < c.nReduce && i < len(req.Task.Files); i++ {
				c.redFiles[i] = append(c.redFiles[i], req.Task.Files[i])
			}
			if c.idleMapTasks.Empty() && c.runningMapTasks.Empty() {
				log.Printf("The map tasks were all done. Start to do reduce.\n")
				c.curPhase = RedPhase
				c.GenerateRedTasks()
			}
		}
	} else {
		c.runningRedTasks.Erase(coTask)
		if result == ProcessSucess && c.idleRedTasks.Empty() && c.runningRedTasks.Empty() {
			log.Printf("The reduce tasks were all done.\n")
			c.curPhase = AllDone
		}
	}
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
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	// Your code here.
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.curPhase == AllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		nextMapTaskId: 0,
		nextWorkerId: 0,
		idleMapTasks: coTaskSet{},
		runningMapTasks: coTaskSet{},
		idleRedTasks: coTaskSet{},
		runningRedTasks: coTaskSet{},
		workers: map[int]*coWorkerInfo{},
		idleWorkerId: map[int]struct{}{},
		mtx: sync.Mutex{},
		nextTaskId: 0,
		curPhase: MapPhase,
		tasks: map[int]*coTaskInfo{},
		redFiles: map[int][]string{},
	}

	// Your code here.
	for _, file := range files {
		c.GenerateMapTask(file)
	}

	c.server()
	return &c
}

func (c *Coordinator) GenerateRedTasks() {
	for redId, files := range c.redFiles {
		task := TaskInfo {
			TaskId: c.nextTaskId,
			TaskType: RedTask,
			NReduce: c.nReduce,
			Files: files,
			MapId: InvalidMapId,
			RedId: redId,
		}
		c.nextTaskId++
		coTask := coTaskInfo {
			BaseInfo: task,
			Stat: coIdle,
			DealingWorker: InvalidWorkerId,
			StartTime: time.Now(),
		}
		c.idleRedTasks.Insert(&coTask)
		c.tasks[task.TaskId] = &coTask
		log.Printf("generated reduce task: %+v.\n", coTask)
	}
}
