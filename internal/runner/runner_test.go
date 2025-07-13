package runner

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/SapphireNick/task-sentinel/internal/dag"
	"github.com/stretchr/testify/assert"
)

var testContext = context.Background()

func ptr[T any](v T) *T {
	return &v
}

func buildTask(id string, deps []string, command string) dag.Task {
	return dag.Task{
		TaskId:     id,
		Type:       dag.TaskTypeShell,
		Command:    command,
		DependsOn:  deps,
		Retries:    ptr(2),
		RetryDelay: ptr(dag.TaskDuration(2 * time.Second)),
		Timeout:    ptr(dag.TaskDuration(2 * time.Second)),
	}
}

func TestExecuteDagAllSuccess(t *testing.T) {
	r := NewRunner(5*time.Second, 2)
	tasks := []dag.Task{
		buildTask("A", nil, "echo ok"),
		buildTask("B", []string{"A"}, "echo ok"),
		buildTask("C", []string{"A"}, "echo ok"),
	}

	results, err := r.ExecuteDag(testContext, tasks)
	assert.NoError(t, err)
	assert.Len(t, results, 3)
	for _, res := range results {
		assert.True(t, res.Success, "task %s should succeed", res.TaskId)
		assert.Contains(t, res.Output, "ok")
	}
}

func TestExecuteDagSkipOnFailure(t *testing.T) {
	r := NewRunner(5*time.Second, 2)
	tasks := []dag.Task{
		buildTask("A", nil, "sh -c 'exit 1'"),
		buildTask("B", []string{"A"}, "echo ok"),
		buildTask("C", []string{"A"}, "echo ok"),
	}

	results, err := r.ExecuteDag(testContext, tasks)
	assert.NoError(t, err)
	assert.Len(t, results, 3)

	// A fails, B and C are skipped
	assert.False(t, results[0].Success)
	assert.False(t, results[1].Success)
	assert.False(t, results[2].Success)
	assert.Empty(t, results[1].Output)
	assert.Empty(t, results[2].Output)
}

func TestExecuteDagConcurrencyLimit(t *testing.T) {
	r := NewRunner(5*time.Second, 2)

	tasks := []dag.Task{}
	for i := range 5 {
		id := string('A' + rune(i))
		tasks = append(tasks, buildTask(id, nil, "sleep 1"))
	}

	results, err := r.ExecuteDag(testContext, tasks)
	assert.NoError(t, err)
	assert.Len(t, results, 5)

	type event struct {
		time    time.Time
		isStart bool
	}

	var events []event
	for _, r := range results {
		events = append(events, event{time: r.StartTime, isStart: true})
		events = append(events, event{time: r.EndTime, isStart: false})
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].time.Equal(events[j].time) {
			return !events[i].isStart && events[j].isStart
		}
		return events[i].time.Before(events[j].time)
	})

	// Sweep through events to find maximum overlap
	maxOverlap := 0
	currentOverlap := 0

	for _, event := range events {
		if event.isStart {
			currentOverlap++
			if currentOverlap > maxOverlap {
				maxOverlap = currentOverlap
			}
		} else {
			currentOverlap--
		}
	}

	assert.LessOrEqual(t, maxOverlap, 2, "max concurrent tasks should not exceed limit")
}
