package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	activeWorks       map[string]*Work
	mapTasks          []*Task
	reduceTasks       []*Task
	mapActiveWorks    int
	reduceActiveWorks int
	nReduce           int
	mu                sync.Mutex
	doneCond          *sync.Cond
	filesMu           sync.Mutex
	started           bool
	allDone           bool
}

func (m *Master) Init(files []string, nReduce int) {
	m.activeWorks = map[string]*Work{}
	m.mapActiveWorks = 0
	m.reduceActiveWorks = 0
	m.mapTasks = []*Task{}
	m.reduceTasks = []*Task{}
	m.nReduce = nReduce
	m.started = true
	m.doneCond = sync.NewCond(&m.mu)
}

var (
	startTime time.Time
)

func (m *Master) CreateTasks(files []string) {
	reduceTaskFiles := []string{}

	for i := 0; i < m.nReduce; i++ {
		out := BASE_FILES + "output-" + strconv.Itoa(i+1)
		reduceFile := BASE_FILES + "mr-reduce-in-" + strconv.Itoa(i+1)
		reduceTaskFiles = append(reduceTaskFiles, reduceFile)

		if _, e := os.Create(out); e != nil {
			log.Printf("[Master] Create [%s] File Error", out)
		}

		if _, e := os.Create(reduceFile); e != nil {
			log.Printf("[Master] Create [%s] File Error", reduceFile)
		}
	}

	m.mu.Lock()
	m.AddTasks(MAP, files...)
	m.AddTasks(REDUCE, reduceTaskFiles...)
	m.mu.Unlock()
}

//action = map or reduce
func (m *Master) AddTasks(action string, files ...string) {
	var tasks []*Task

	for _, file := range files {
		tasks = append(tasks, &Task{
			Action:         action,
			File:           file,
			TempToResFiles: map[string]string{},
		})
	}

	if action == MAP {
		m.mapTasks = append(tasks, m.mapTasks...)
	} else {
		m.reduceTasks = append(m.reduceTasks, tasks...)
	}
}

func (m *Master) RemoveActiveWork(work *Work) {
	if _, ok := m.activeWorks[work.Id]; ok {
		delete(m.activeWorks, work.Id)
		if work.Task.Action == MAP {
			m.mapActiveWorks -= 1
		} else if work.Task.Action == REDUCE {
			m.reduceActiveWorks -= 1
		}
	}
}

func (m *Master) Checker() {
	for {
		m.mu.Lock()

		fmt.Println("Current Status: ",
			"activeWorks : ", len(m.activeWorks),
			"| mapTasks : ", len(m.mapTasks),
			"| reduceTasks : ", len(m.reduceTasks),
			"| mapActiveWorks : ", m.mapActiveWorks,
			"| reduceActiveWorks : ", m.reduceActiveWorks,
		)
		for _, w := range m.activeWorks {
			if time.Now().After(w.Timeout) {
				// fmt.Println("Work timeout... reassigning task")
				m.RemoveActiveWork(w)
				m.AddTasks(w.Task.Action, w.Task.File)
			}
		}

		m.allDone = m.mapActiveWorks == 0 && m.reduceActiveWorks == 0 && len(m.mapTasks) == 0 && len(m.reduceTasks) == 0 && m.started

		if m.allDone {
			m.doneCond.Broadcast()
			m.mu.Unlock()
			// fmt.Println("Checker Done!")
			return
		}

		m.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (m *Master) CreateNewWork() *Work {
	var task *Task

	if len(m.mapTasks) > 0 {
		task = m.mapTasks[0]
		m.mapTasks = m.mapTasks[1:]
		m.mapActiveWorks += 1
	} else if len(m.mapTasks) == 0 && m.mapActiveWorks == 0 && len(m.reduceTasks) > 0 {
		task = m.reduceTasks[0]
		m.reduceTasks = m.reduceTasks[1:]
		m.reduceActiveWorks += 1
	}

	var work *Work

	if task != nil {
		work = &Work{
			Id:      strconv.FormatInt(time.Now().UnixNano(), 10),
			Status:  WORKING,
			Timeout: time.Now().Add(time.Second * 10),
			Task:    task,
		}
		m.activeWorks[work.Id] = work
	}
	return work
}

func (m *Master) UpdateFiles(task *Task) {
	m.filesMu.Lock()
	if task.Action == MAP {
		for t, o := range task.TempToResFiles {
			temp, err := os.Open(t)
			if err != nil {
				fmt.Println("Cannot open file", t)
			}
			out, err := os.OpenFile(o, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			if err != nil {
				fmt.Println("Cannot open file", o)
			}

			tempDecoder := json.NewDecoder(temp)
			outEncoder := json.NewEncoder(out)

			for {
				var kv KeyValue
				if err := tempDecoder.Decode(&kv); err != nil {
					break
				}
				enc := outEncoder.Encode(&kv)
				if enc != nil {
					fmt.Println("enc error ", enc)
				}
			}
			temp.Close()
			out.Close()
		}
	} else if task.Action == REDUCE {
		for t, o := range task.TempToResFiles {
			temp, err := os.Open(t)
			if err != nil {
				fmt.Println("Cannot open file", t)
			}
			out, err := os.OpenFile(o, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			if err != nil {
				fmt.Println("Cannot open file", o)
			}
			tempDecoder := json.NewDecoder(temp)

			for {
				var kv KeyValue
				if err := tempDecoder.Decode(&kv); err != nil {
					break
				}
				fmt.Fprintf(out, "%v %v\n", kv.Key, kv.Value)
			}

			temp.Close()
			out.Close()
		}
	}
	m.filesMu.Unlock()
	m.CleanWorkerFiles(task)
}

func (m *Master) CleanWorkerFiles(task *Task) {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	for t, _ := range task.TempToResFiles {
		err := os.Remove(t)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func (m *Master) Sync(work *Work, response *SyncResponse) error {
	// fmt.Println("Worker asking for Sync worker status: ", work.Status)
	m.mu.Lock()
	defer m.mu.Unlock()

	switch work.Status {

	case IDLE:
		response.NewWork = m.CreateNewWork()

	case DONE:
		if _, ok := m.activeWorks[work.Id]; ok {
			m.UpdateFiles(work.Task)
			m.RemoveActiveWork(work)
			response.NewWork = m.CreateNewWork()
		} else {
			fmt.Println("******Dead worker is alive again", work.Task)
			go m.CleanWorkerFiles(work.Task)
			response.NewWork = m.CreateNewWork()
		}
	}

	m.allDone = m.mapActiveWorks == 0 && m.reduceActiveWorks == 0 && len(m.mapTasks) == 0 && len(m.reduceTasks) == 0 && m.started
	response.AllDone = m.allDone
	response.NReduce = m.nReduce

	return nil
}

func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for !m.allDone {
		m.doneCond.Wait()
	}

	for i := 0; i < m.nReduce; i++ {
		out := BASE_FILES + "mr-reduce-in-" + strconv.Itoa(i+1)
		err := os.Remove(out)
		if err != nil {
			fmt.Println(err)
		}
	}

	return true
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	startTime = time.Now()

	m.Init(files, nReduce)
	m.CreateTasks(files)
	fmt.Println("Init done")
	go m.Checker()

	m.server()
	return &m
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: master inputfiles...\n")
		os.Exit(1)
	}

	m := MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
