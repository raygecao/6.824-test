package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MergedKeyValues struct {
	Key    string
	Values []string
}

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
		var resp AcquireWorkResponse
		// If Master is done or crash, call will fatal which makes worker stop (This is what we are required)
		call("Master.AcquireWork", new(AcquireWorkRequest), &resp)
		fmt.Println(resp)
		if !resp.Acquired {
			// wait for another request, maybe for wait slow node or wait for fault tolerance
			time.Sleep(time.Second)
			continue
		}
		req := FinishWorkRequest{ID: resp.ID}
		switch resp.WorkType {
		case MAP:
			fmt.Println("process map")
			processMapTask(mapf, resp.Files, resp.NReduce, resp.ID)
			req.WorkType = MAP
			call("Master.FinishWork", req, new(FinishWorkResponse))
		case REDUCE:
			fmt.Println("process reduce")
			processReduceTask(reducef, resp.Files, resp.ID)
			req.WorkType = REDUCE
			call("Master.FinishWork", req, new(FinishWorkResponse))
		default:
			log.Fatalf("unknown work type %v", resp.WorkType)
		}
	}

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

func getFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func processMapTask(mapf func(string, string) []KeyValue, filenames []string, nReduce int, mapID int) {
	partitions := make([][]KeyValue, nReduce)
	for _, fileName := range filenames {
		content := getFileContent(fileName)
		kva := mapf(fileName, content)
		for _, kv := range kva {
			idx := ihash(kv.Key) % nReduce
			partitions[idx] = append(partitions[idx], kv)
		}
	}

	for i := 0; i < nReduce; i++ {
		mkvs := mapShuffle(partitions[i])
		interFile := fmt.Sprintf("mr-%d-%d", mapID, i)
		iFile, err := ioutil.TempFile(os.TempDir(), "map")
		if err != nil {
			log.Fatal("fail to create tmp file in mapper", err)
		}
		enc := json.NewEncoder(iFile)
		for _, mkv := range mkvs {
			if err := enc.Encode(&mkv); err != nil {
				log.Fatal("json encode fail", err)
			}
		}
		os.Rename(iFile.Name(), interFile)
		iFile.Close()
	}
}

func processReduceTask(reducef func(string, []string) string, iFiles []string, reduceID int) {
	var mkvs []MergedKeyValues
	for _, fileName := range iFiles {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("fail to open intermedia %s", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var mkv MergedKeyValues
			if err := dec.Decode(&mkv); err != nil {
				break
			}
			mkvs = append(mkvs, mkv)
		}
	}
	if mkvs == nil {
		return
	}
	sort.Sort(ByMergedKey(mkvs))

	oname := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, err := ioutil.TempFile(os.TempDir(), "reduce")
	if err != nil {
		log.Fatal("fail to create a tmp file in reducer", err)
	}

	lastKey := mkvs[0].Key
	var values []string
	// reduce shuffle
	for _, kv := range mkvs {
		if kv.Key == lastKey {
			values = append(values, kv.Values...)
			continue
		}
		output := reducef(lastKey, values)
		fmt.Fprintf(ofile, "%v %v\n", lastKey, output)
		lastKey = kv.Key
		values = kv.Values
	}
	output := reducef(lastKey, values)
	fmt.Fprintf(ofile, "%v %v\n", lastKey, output)
	os.Rename(ofile.Name(), oname)
	ofile.Close()
}

func mapShuffle(kvs []KeyValue) []MergedKeyValues {
	if kvs == nil {
		return nil
	}
	sort.Sort(ByKey(kvs))
	lastKey := kvs[0].Key
	var mkvs []MergedKeyValues
	var values []string
	for _, kv := range kvs {
		if kv.Key == lastKey {
			values = append(values, kv.Value)
			continue
		}
		// merge laskKey values
		mkvs = append(mkvs, MergedKeyValues{lastKey, values})
		lastKey = kv.Key
		values = []string{kv.Value}
	}
	// merge values of the final key
	mkvs = append(mkvs, MergedKeyValues{lastKey, values})
	return mkvs
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// TODO: replace ByKey
type ByMergedKey []MergedKeyValues

// for sorting by key.
func (a ByMergedKey) Len() int           { return len(a) }
func (a ByMergedKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByMergedKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
