package main

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io/ioutil"
import "strconv"
import "time"
import "sort"


// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


type meta struct {
	work    *Work
	NReduce int
	AllDone bool
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (m *meta) Sync() {
	for {

		response := SyncResponse{}

		if !call("Master.Sync", m.work, &response) {
			//s.Init(s.mapf,s.reducef)
			fmt.Println("os.Exit(0) from worker")
			os.Exit(0)
		}

		m.NReduce = response.NReduce

		if response.AllDone {
			fmt.Println("Worker Done!")
			return
		}
		if response.NewWork != nil {
			m.work = response.NewWork
			m.DoWork()
			// fmt.Println("Done Task =>", m.work)
		} else {
			m.work = &Work{
				Status: IDLE,
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (m *meta) Map() {
	fmt.Println("Map -> ", m.work.Task.File)
	defer fmt.Println("Map done-> ", m.work.Task.File)

	task := m.work.Task
	file, err := os.Open(task.File)
	if err != nil {
		fmt.Println("Cannot open file", file)
	}
	content, err := ioutil.ReadAll(file)

	if err != nil {
		fmt.Println("Reading error", file)
	}

	file.Close()

	kvs := Mapf(task.File, string(content))

	var fileToKvs map[string][]KeyValue

	fileToKvs = map[string][]KeyValue{}

	for _, kv := range kvs {
		outf := BASE_FILES + "mr-reduce-in-" + strconv.Itoa(ihash(kv.Key)%m.NReduce+1)

		if _, ok := fileToKvs[outf]; !ok {
			fileToKvs[outf] = []KeyValue{}
		}

		fileToKvs[outf] = append(fileToKvs[outf], kv)
	}

	for out, kvs := range fileToKvs {
		tempFile := BASE_FILES + strconv.FormatInt(time.Now().UnixNano(), 10)
		if _, e := os.Create(tempFile); e != nil {
			log.Printf("[Master] Create [%s] File Error", tempFile)
		}
		f, err := os.OpenFile(tempFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

		if err != nil {
			fmt.Println("Cannot open file ", tempFile)
		}
		encoder := json.NewEncoder(f)
		for _, kv := range kvs {
			enc := encoder.Encode(&kv)
			if enc != nil {
				fmt.Println("enc error ", enc)
			}
		}

		task.TempToResFiles[tempFile] = out

		f.Close()
	}
}

func (m *meta) Reduce() {
	fmt.Println("Reduce -> ", m.work.Task.File)
	defer fmt.Println("Reduce done -> ", m.work.Task.File)

	task := m.work.Task
	file, err := os.Open(task.File)
	if err != nil {
		fmt.Println("Cannot open file", file)
	}
	// content, err := ioutil.ReadAll(file)

	decoder := json.NewDecoder(file)
	intermediate := []KeyValue{}

	for {
		var kv KeyValue
		if err := decoder.Decode(&kv); err != nil {
			break
		}
		intermediate = append(intermediate, kv)
	}

	sort.Sort(ByKey(intermediate))

	var fileToKvs map[string][]KeyValue

	fileToKvs = map[string][]KeyValue{}

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
		output := Reducef(intermediate[i].Key, values)

		outf := BASE_FILES + "output-" + strconv.Itoa(ihash(intermediate[i].Key)%m.NReduce+1)

		if _, ok := fileToKvs[outf]; !ok {
			fileToKvs[outf] = []KeyValue{}
		}

		fileToKvs[outf] = append(fileToKvs[outf], KeyValue{Key: intermediate[i].Key, Value: output})

		i = j
	}

	for out, kvs := range fileToKvs {
		tempFile := BASE_FILES + strconv.FormatInt(time.Now().UnixNano(), 10)
		if _, e := os.Create(tempFile); e != nil {
			log.Printf("[Master] Create [%s] File Error", tempFile)
		}
		f, err := os.OpenFile(tempFile, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			fmt.Println("Cannot open file ", tempFile)
		}

		encoder := json.NewEncoder(f)
		for _, kv := range kvs {
			enc := encoder.Encode(&kv)
			if enc != nil {
				fmt.Println("enc error ", enc)
			}
		}

		task.TempToResFiles[tempFile] = out
		f.Close()
	}

}

func (m *meta) DoWork() {
	if t := m.work.Task; t != nil {
		if t.Action == MAP {
			m.Map()
			m.work.Status = DONE
		} else if t.Action == REDUCE {
			m.Reduce()
			m.work.Status = DONE
		}
	}
}

func Worker() {

	m := meta{}

	m.work = &Work{
		Status: IDLE,
	}

	m.Sync()

	fmt.Println("Worker shutting down.....")
}

func call(rpcname string, args interface{}, reply interface{}) bool {
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

func main() {
	Worker()
}