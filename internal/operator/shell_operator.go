package operator

import (
	"context"
	"os/exec"
	"time"

	"github.com/SapphireNick/task-sentinel/internal/dag"
)

type ShellOperator struct{}

func NewShellOperator() Operator {
	return &ShellOperator{}
}

func (o *ShellOperator) ExecuteTask(ctx context.Context, task *dag.Task) TaskExecutionResult {
	taskCtx, cancel := context.WithTimeout(ctx, time.Duration(*task.Timeout))
	defer cancel()

	cmd := exec.CommandContext(taskCtx, "bash", "-c", task.Command)
	output, err := cmd.CombinedOutput()

	return TaskExecutionResult{
		TaskId:  task.TaskId,
		Success: err == nil,
		Output:  string(output),
		Error:   err,
	}
}
