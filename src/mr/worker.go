package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "sort"
import "encoding/json"
import "strconv"


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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	log.Printf("Registering worker...\n")
	workerInfo := CallRegisterWorker()
	workerId := workerInfo.Id
	log.Printf("worker %d registered sucessfully.\n", workerId)
	for {
		log.Printf("worker %d asking for a task...\n", workerId)
		task := CallAskForTask(workerInfo)
		if task.TaskId == InvalidTaskId {
			log.Printf("worker %d:The server has no available task. 1s later ask for new task.\n", workerId)
			time.Sleep(1 * time.Second)
			continue
		} else {
			log.Printf("worker %d received a task:\n%+v\n", workerId, task)
		}
		// processTask will store the file names in task info.
		result := processTask(workerInfo, &task, mapf, reducef)
		CallReportTask(workerInfo, task, result)
	}
}

func CallAskForTask(worker WorkerInfo) TaskInfo {
	req := AskForTaskReq{worker}
	rsp := AskForTaskRsp{}
	call("Coordinator.AskForTask", &req, &rsp)
	return rsp.Task
}

func CallRegisterWorker() WorkerInfo {
	req := RegisterWorkerReq{}
	rsp := RegisterWorkerRsp{}
	call("Coordinator.RegisterWorker", &req, &rsp)
	return rsp.Info
}

func CallReportTask(worker WorkerInfo, task TaskInfo, result ProcessResultType) {
	req := ReportTaskReq{}
	rsp := ReportTaskRsp{}
	req.Worker = worker
	req.Task = task
	req.Result = result
	call("Coordinator.ReportTask", &req, &rsp)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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

func processTask(worker WorkerInfo, task *TaskInfo, mapf func(string, string) []KeyValue,
	reducef func(string, []string)string) ProcessResultType {
	if task.TaskType == MapTask {
		intermediate := []KeyValue{}
		for _, filename := range task.Files {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				return ProcessFailed
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
				return ProcessFailed
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
		}
		sort.Sort(ByKey(intermediate))
		nReduce := task.NReduce
		outFiles := make([]*os.File, nReduce)
		fileEncs := make([]*json.Encoder, nReduce)
		for outIdx := 0; outIdx < nReduce; outIdx++ {
			f, err := ioutil.TempFile("./", "mr-tmp-*")
			if err != nil {
				log.Fatalf("cannot create temp file: %v", err)
				return ProcessFailed
			}
			outFiles[outIdx] = f
			fileEncs[outIdx] = json.NewEncoder(outFiles[outIdx])
		}
		for _, kv := range intermediate {
			outIdx := ihash(kv.Key) % nReduce
			err := fileEncs[outIdx].Encode(kv)
			if err != nil {
				log.Printf("worker %d: encoding json failed.\n", worker.Id)
				for _, file := range outFiles {
					file.Close()
					os.Remove(file.Name())
				}
				return ProcessFailed
			}
		}
		outPrefix := fmt.Sprintf("./mr-%d-", task.MapId)
		files := []string{}
		for outIdx, file := range outFiles {
			outName := outPrefix + strconv.Itoa(outIdx)
			oldPath := outFiles[outIdx].Name()
			os.Rename(oldPath, outName)
			files = append(files, outName)
			log.Printf("renamed the tmp file %s to %s", oldPath, outName)
			file.Close()
		}
		task.Files = files
	} else {
		// Reduce
		redId := task.RedId
		kva := []KeyValue{}
		for _, filename := range task.Files {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				return ProcessFailed
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
		sort.Sort(ByKey(kva))
		f, err := ioutil.TempFile("./", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create tmp file: %v.\n", err)
			return ProcessFailed
		}
		log.Printf("created a temp reduce file: %s.\n", f.Name())
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)
			i = j
		}
		f.Close()
		oname := fmt.Sprintf("mr-out-%d", redId)
		os.Rename(f.Name(), oname)
		log.Printf("renamed temp file %s to %s", f.Name(), oname)
		task.Files = []string{oname}
	}
	return ProcessSucess
}
