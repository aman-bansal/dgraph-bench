package main

import (
	"flag"
	"fmt"
	"github.com/dgraph-io/dgo/v200"
	"github.com/dgraph-io/dgo/v200/protos/api"
	"github.com/linuxerwang/dgraph-bench/tasks"
	"google.golang.org/grpc"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	flagServers = flag.String("servers", "", "Comma separated dgraph server endpoints")

	dgraphCli *dgo.Dgraph
)

func usage() {
	fmt.Println("Usage:")
	fmt.Println("\tdgraph-bench")
	fmt.Println()
	flag.PrintDefaults()
	fmt.Println()
}

func connect(servers string) *dgo.Dgraph {
	clis := make([]api.DgraphClient, 0, 5)
	for _, s := range strings.Split(strings.Replace(servers, " ", "", -1), ",") {
		if len(s) > 0 {
			fmt.Printf("Connect to server %s\n", s)
			conn, err := grpc.Dial(s, grpc.WithInsecure())
			if err != nil {
				panic(err)
			}
			clis = append(clis, api.NewDgraphClient(conn))
		}
	}
	return dgo.NewDgraphClient(clis...)
}

type BenchCase struct {
	Name        string `yaml:"name"`
	Concurrency int    `yaml:"concurrency"`
	Time        int64    `yaml:"time"` // in minute
}

type Config struct {
	Execution  string       `yaml:"execution"`
	BenchCases []*BenchCase `yaml:"bench_cases"`
}

func loadConfig(fn string) *Config {
	b, err := ioutil.ReadFile(fn)
	if err != nil {
		panic(err)
	}

	conf := Config{}
	if err := yaml.Unmarshal([]byte(b), &conf); err != nil {
		log.Fatalf("error: %v", err)
	}
	return &conf
}

func main() {
	flag.Usage = usage
	flag.Parse()

	go tasks.StartPrometheusServer(3500)

	if *flagServers == "" {
		fmt.Println("Flag --servers is required.")
		os.Exit(2)
	}
	dgraphCli = connect(*flagServers)

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	conf := loadConfig(flag.Arg(0))
	fmt.Printf("Found %d tasks.\n", len(conf.BenchCases))
	if conf.Execution == "serial" {
		runBenchmarkCaseSerial(conf)
	} else if conf.Execution == "parallel" {
		runBenchmarkCaseParallel(conf)
	}
}

func runBenchmarkCaseParallel(conf *Config) {
	wg := sync.WaitGroup{}
	wg.Add(len(conf.BenchCases))
	for _, bench := range conf.BenchCases {
		go func(c *BenchCase) {
			runBenchmarkCaseSerial(&Config{
				Execution:  "serial",
				BenchCases: []*BenchCase{c},
			})
			wg.Done()
		}(bench)
	}

	wg.Wait()
}

func runBenchmarkCaseSerial(conf *Config) {
	for _, bench := range conf.BenchCases {
		closeChan := make([]chan int, bench.Concurrency+1)
		for i := 0; i <= bench.Concurrency; i++ {
			closeChan[i] = make(chan int)
		}
		task, ok := tasks.BenchTasks[bench.Name]
		if !ok {
			log.Fatalf("Task not found: %s\n", bench.Name)
		}
		fmt.Printf("Starting task %s (%d concurrent goroutines)\n", bench.Name, bench.Concurrency)
		go tasks.ExecTask(bench.Name, task, dgraphCli, bench.Concurrency, closeChan)
		//close all go routines and start next task
		select {
		case <-time.Tick(time.Duration(bench.Time) * time.Minute):
			for i := 0; i < bench.Concurrency; i++ {
				closeChan[i] <- i
			}
			time.Sleep(5 * time.Second)
			closeChan[bench.Concurrency] <- bench.Concurrency

		}
		for i := 0; i <= bench.Concurrency; i++ {
			close(closeChan[i])
		}
		// adding sleep so that it will be visible in charts when the query changes
		fmt.Printf("All done for benchcase %s\n", bench.Name)
		time.Sleep(10 * time.Minute)
	}
}
