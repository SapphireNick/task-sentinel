package operator

import (
	"context"
	"testing"
	"time"

	"github.com/SapphireNick/task-sentinel/internal/dag"
)

func ptr[T any](v T) *T {
	return &v
}

func TestShellOperatorExecuteTaskSuccess(t *testing.T) {
	op := &ShellOperator{}
	task := &dag.Task{
		TaskId:  "echo-test",
		Command: `echo "hello world"`,
		Timeout: ptr(dag.TaskDuration(2 * time.Second)),
	}

	result := op.ExecuteTask(context.Background(), task)

	if !result.Success {
		t.Errorf("expected success, got failure: %v", result.Error)
	}
	if result.Output != "hello world\n" {
		t.Errorf("unexpected output: %s", result.Output)
	}
}

func TestShellOperatorExecuteTaskFailure(t *testing.T) {
	op := &ShellOperator{}
	task := &dag.Task{
		TaskId:  "fail-test",
		Command: `exit 1`,
		Timeout: ptr(dag.TaskDuration(2 * time.Second)),
	}

	result := op.ExecuteTask(context.Background(), task)

	if result.Success {
		t.Errorf("expected failure, got success")
	}
	if result.Error == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestShellOperatorExecuteTaskTimeout(t *testing.T) {
	op := &ShellOperator{}
	task := &dag.Task{
		TaskId:  "timeout-test",
		Command: `sleep 5`,
		Timeout: ptr(dag.TaskDuration(1 * time.Second)),
	}

	start := time.Now()
	result := op.ExecuteTask(context.Background(), task)
	elapsed := time.Since(start)

	if result.Success {
		t.Errorf("expected timeout failure, got success")
	}
	if result.Error == nil {
		t.Errorf("expected timeout error, got nil")
	}
	if elapsed > 2*time.Second {
		t.Errorf("expected timeout within 1s, took too long: %v", elapsed)
	}
}
