package runner

import (
	"context"
	"time"

	"github.com/SapphireNick/task-sentinel/internal/dag"
	"github.com/SapphireNick/task-sentinel/internal/operator"
)

type Runner interface {
	ExecuteDag(ctx context.Context, tasks []dag.Task) ([]DagExecutionResult, error)
}

type DagExecutionResult struct {
	operator.TaskExecutionResult
	Duration  time.Duration
	StartTime time.Time
	EndTime   time.Time
}

type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusRunning
	TaskStatusCompleted
	TaskStatusFailed
)
