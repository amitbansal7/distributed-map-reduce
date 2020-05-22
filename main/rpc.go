package main

import (
	"os"
	"strconv"
	"time"
	"strings"
	"unicode"
)

const BASE_FILES = "files/"

//Status
const(
	IDLE = "Idle"
	DONE = "Done"
	WORKING = "Working"
)

//Task
const(
	MAP = "Map"
	REDUCE = "Reduce"
)

type KeyValue struct {
	Key   string
	Value string
}


type Work struct {
	Id      string
	Status  string
	Timeout time.Time
	Task    *Task
}

type Task struct {
	Action         string
	File           string
	TempToResFiles map[string]string
}

type SyncResponse struct {
	NewWork *Work
	NReduce int
	AllDone bool
}

func Mapf(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reducef(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}


func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}