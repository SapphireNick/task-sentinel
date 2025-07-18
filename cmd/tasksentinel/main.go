package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/SapphireNick/task-sentinel/internal/dag"
	"github.com/SapphireNick/task-sentinel/internal/runner"
)

func main() {
	var (
		dag_path = flag.String("dag", "", "(required) Path to Dag config (.json)")
	)

	flag.Parse()

	if *dag_path == "" {
		flag.Usage()
		os.Exit(1)
	}

	parser := dag.NewJSONParser()
	if _, err := os.Stat(*dag_path); os.IsNotExist(err) {
		fmt.Printf("Error: path '%s' does not exist\n", *dag_path)
		os.Exit(1)
	}

	file, err := os.Open(*dag_path)
	if err != nil {
		fmt.Printf("Error: failed to open %s", *dag_path)
		os.Exit(1)
	}

	dag, err := parser.Parse(file)
	if err != nil {
		fmt.Printf("Error: failed to parse dag config %s", err.Error())
		os.Exit(1)
	}

	runner := runner.NewRunner(time.Duration(*dag.DefaultArgs.Timeout), dag.MaxActiveRuns)
	ctx := context.Background()

	run_result, err := runner.ExecuteDag(ctx, dag.Tasks)
	if err != nil {
		fmt.Printf("Error: failed to execute dag %s", err.Error())
		os.Exit(1)
	}

	for _, res := range run_result {
		fmt.Printf("res: %v\n", res)
	}
}
