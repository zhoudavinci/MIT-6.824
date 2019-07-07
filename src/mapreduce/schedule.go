package mapreduce

import (
  "fmt"
  "sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
  switch phase {
  case mapPhase:
    // Be sure of all the tasks has been done.
    wg := sync.WaitGroup{}
    wg.Add(ntasks)
    for i := 0; i < ntasks; i++ {
      fmt.Println("map task id: ", i)
      taskArgs := DoTaskArgs{jobName, mapFiles[i], mapPhase, i, n_other}
      // get worker's address, use channel for Goroutine func with return value
      reg := make(chan string)
      go getRegister(registerChan, reg)
      register := <-reg

      // call rpc for tasks with Goroutine, meantime, pushing the worker's address into the channel after the task has been done.
      go callRpc(register, registerChan, taskArgs, &wg)
    }
    wg.Wait()
  case reducePhase:
    wg := sync.WaitGroup{}
    wg.Add(ntasks)
    for i := 0; i < ntasks; i++ {
      fmt.Println("reduce task id: ", i)
      taskArgs := DoTaskArgs{jobName, "", reducePhase, i, n_other}

      reg := make(chan string)
      go getRegister(registerChan, reg)
      register := <-reg

      go callRpc(register, registerChan, taskArgs, &wg)
    }
    wg.Wait()
  }

	fmt.Printf("Schedule: %v done\n", phase)
}

// use call() to do map/reduce task, and push the worker's address into channel for next map/reduce task. the wg should be a pointer, otherwise the process would be dead-lock status.
func callRpc(register string, registerChan chan string, taskArgs DoTaskArgs, wg *sync.WaitGroup) {
  ok := call(register, "Worker.DoTask", taskArgs, nil)
  if (ok == false) {
    fmt.Println("map/reduce task error.")
  }
  wg.Done()
  setRegister(registerChan, register)
}

func getRegister(register chan string, res chan string) {
  tmp := <-register
  res <- tmp
}

func setRegister(register chan string, res string) {
  register <- res
}
