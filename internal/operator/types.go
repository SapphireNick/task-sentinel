package operator

import (
	"context"

	"github.com/SapphireNick/task-sentinel/internal/dag"
)

type Operator interface {
	ExecuteTask(ctx context.Context, task *dag.Task) TaskExecutionResult
}

type TaskExecutionResult struct {
	TaskId  string
	Success bool
	Output  string
	Error   error
}

var TaskTypeToOperator = map[dag.TaskType]Operator{
	dag.TaskTypeShell: &ShellOperator{},
}
