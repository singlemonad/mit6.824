package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	mutex := sync.Mutex{}
	dones := make(map[int]int)
	for i := 0; i < ntasks; i++ {
		dones[i] = 0
	}
	shouldExit := func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		for _, v := range dones {
			if v != 2 {
				return false
			}
		}
		return true
	}

	wg := sync.WaitGroup{}
	for !shouldExit() {
		todo := 0
		found := false
		mutex.Lock()
		for k, v := range dones {
			if v == 0 {
				dones[k] = 1
				todo = k
				found = true
				break
			}
		}
		mutex.Unlock()
		if !found {
			break
		}

		worker := <-mr.registerChannel
		wg.Add(1)
		go func(taskNumber int, worker string) {
			defer wg.Done()

			arg := &DoTaskArgs{
				JobName:       mr.jobName,
				File:          mr.files[taskNumber],
				Phase:         phase,
				TaskNumber:    taskNumber,
				NumOtherPhase: nios,
			}
			success := call(worker, "Worker.DoTask", arg, new(struct{}))
			if !success {
				fmt.Printf("Worker: %s occur error\n", worker)
				mutex.Lock()
				dones[todo] = 0
				mutex.Unlock()
			} else {
				mutex.Lock()
				dones[todo] = 2
				mutex.Unlock()
				mr.Register(&RegisterArgs{
					Worker: worker,
				}, new(struct{}))
			}
		}(todo, worker)
	}
	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
