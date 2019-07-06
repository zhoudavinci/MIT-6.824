package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
  "log"
)

type KeyValueSlice []KeyValue

func (kv KeyValueSlice) Len() int {
	return len(kv)
}

func (kv KeyValueSlice) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

func (kv KeyValueSlice) Less(i, j int) bool {
  return kv[i].Key < kv[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	var kvs KeyValueSlice
  // 1. read all the reduce file and decode the data stream
  for m := 0; m < nMap; m++ {
    inFile := reduceName(jobName, m, reduceTask)
	  file, err := os.Open(inFile)
	  if err != nil {
		  log.Fatal("Open file failed, ", err)
		  return
	  }
	  defer file.Close()

	  // build decoder
	  dec := json.NewDecoder(file)

    for dec.More() {
      var kv KeyValue
      err = dec.Decode(&kv)
      if err != nil {
        log.Fatal("Decoder failed, ", err)
        break
      }
      kvs = append(kvs, kv)
    }
  }

  // 2. sort kvs
	sort.Sort(kvs)

  // 3. for each distinct key, use reduceF to calculate all the values
	file, _ := os.OpenFile(outFile, os.O_CREATE|os.O_RDWR, 0664)
	defer file.Close()

  enc := json.NewEncoder(file)
	length := len(kvs)
	for i := 0; i < length; i++ {
		key := kvs[i].Key
		var values []string
    j := i
		for ; j < length; j++{
			if (kvs[j].Key != key) {
				break
			} else {
				values = append(values, kvs[j].Value)
			}
    }
    i = j - 1
    err := enc.Encode(KeyValue{key, reduceF(key, values)})
    if (nil != err) {
      log.Fatal("Reduce encoder failed, ", err)
    }
	}
}
