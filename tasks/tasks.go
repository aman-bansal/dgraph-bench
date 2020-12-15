package tasks

import (
	"fmt"
	"github.com/dgraph-io/dgo/v200"
	"sync/atomic"
	"time"
)

// BenchmarkCase ...
type BenchmarkCase func(dgraphCli *dgo.Dgraph) error

var (
	BenchTasks = map[string]BenchmarkCase{}
)

func report(name string, count *int64, closeChan chan int) {
	prev := atomic.LoadInt64(count)
	timeCount := 0
	tick := time.Tick(1 * time.Second)
	for {
		select {
		case <-tick:
			timeCount++
			cnt := atomic.LoadInt64(count)
			throughput.WithLabelValues(name, "OK").Set(float64(cnt - prev))
			fmt.Printf("Time elapsed: %d, Taskname: %s, Speed: %d\n", timeCount, name, cnt-prev)
			prev = cnt
		case <-closeChan:
			return
		}
	}
}

func ExecTask(name string, bc BenchmarkCase, dgraphCli *dgo.Dgraph,
	concurrency int, closeChan []chan int) {
	count := int64(0)
	go report(name, &count, closeChan[concurrency])
	for i := 0; i < concurrency; i++ {
		go func(routineId int, cChan chan int) {
			runTask := func() {
				start := time.Now()
				err := bc(dgraphCli)
				d := time.Since(start)

				status := "OK"
				if err != nil {
					status = "ERROR"
				} else {
					atomic.AddInt64(&count, 1)
				}
				counters.WithLabelValues(name, status).Inc()
				durations.WithLabelValues(name, status).Observe(d.Seconds())
			}
			for {
				select {
				case <-cChan:
					return
				default:
					runTask()
				}
			}
		}(i, closeChan[i])
	}
}
