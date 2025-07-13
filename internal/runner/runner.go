package runner

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/SapphireNick/task-sentinel/internal/dag"
	"github.com/SapphireNick/task-sentinel/internal/operator"
)

type runnerImpl struct {
	timeout       time.Duration
	maxConcurrent int

	status   map[string]TaskStatus
	results  map[string]DagExecutionResult
	channels map[string]chan struct{}
	mut      sync.Mutex
}

func NewRunner(timeout time.Duration, maxConcurrent int) Runner {
	return &runnerImpl{
		timeout:       timeout,
		maxConcurrent: maxConcurrent,
		status:        make(map[string]TaskStatus),
		results:       make(map[string]DagExecutionResult),
		channels:      make(map[string]chan struct{}),
	}
}

func (r *runnerImpl) ExecuteDag(ctx context.Context, tasks []dag.Task) ([]DagExecutionResult, error) {
	if len(tasks) == 0 {
		return []DagExecutionResult{}, nil
	}

	for _, task := range tasks {
		r.channels[task.TaskId] = make(chan struct{}, 1)
		r.status[task.TaskId] = TaskStatusPending
	}

	sem := make(chan struct{}, r.maxConcurrent)
	var wg sync.WaitGroup

	for _, task := range tasks {
		wg.Add(1)

		go func(task *dag.Task) {
			defer wg.Done()

			for _, dep := range task.DependsOn {
				<-r.channels[dep]

				r.mut.Lock()
				res := r.results[dep]
				r.mut.Unlock()

				if res.Error != nil {
					r.setStatus(task.TaskId, TaskStatusFailed)
					r.publishResultAndBroadcast(task.TaskId, operator.TaskExecutionResult{
						TaskId:  task.TaskId,
						Success: false,
						Output:  "",
						Error:   errors.New("failed because of failing dependency"),
					}, time.Now())
					return
				}
			}

			sem <- struct{}{}
			defer func() { <-sem }()

			startTime := time.Now()

			r.setStatus(task.TaskId, TaskStatusRunning)

			res := operator.TaskTypeToOperator[task.Type].ExecuteTask(ctx, task)
			if !res.Success {
				r.setStatus(task.TaskId, TaskStatusFailed)
				r.publishResultAndBroadcast(task.TaskId, res, startTime)
				return
			}

			r.setStatus(task.TaskId, TaskStatusCompleted)
			r.publishResultAndBroadcast(task.TaskId, res, startTime)
		}(&task)
	}

	wg.Wait()

	results := make([]DagExecutionResult, 0, len(tasks))
	for _, task := range tasks {
		results = append(results, r.results[task.TaskId])
	}

	return results, nil
}

func (r *runnerImpl) setStatus(taskId string, status TaskStatus) {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.status[taskId] = status
}

func (r *runnerImpl) publishResultAndBroadcast(taskId string, res operator.TaskExecutionResult, startTime time.Time) {
	endTime := time.Now()

	result := DagExecutionResult{
		TaskExecutionResult: res,
		Duration:            endTime.Sub(startTime),
		StartTime:           startTime,
		EndTime:             endTime,
	}

	r.mut.Lock()
	r.results[taskId] = result
	ch := r.channels[taskId]
	r.mut.Unlock()

	close(ch)
}
