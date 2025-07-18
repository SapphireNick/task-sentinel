package dag

import (
	"encoding/json"
	"fmt"
	"time"
)

type Dag struct {
	Id            string    `json:"dag_id"`
	Description   string    `json:"description"`
	Schedule      string    `json:"schedule"`
	StartDate     time.Time `json:"start_date"`
	MaxActiveRuns int       `json:"max_active_runs"`
	DefaultArgs   *TaskArgs `json:"default_args"`
	Tasks         []Task    `json:"tasks"`
}

type TaskArgs struct {
	Retries    *int          `json:"retries"`
	RetryDelay *TaskDuration `json:"retry_delay"`
	Timeout    *TaskDuration `json:"timeout"`
}

type Task struct {
	TaskId    string   `json:"task_id"`
	Type      TaskType `json:"type"`
	Command   string   `json:"command"`
	DependsOn []string `json:"depends_on"`

	// Optional task-specific overrides
	Retries    *int          `json:"retries,omitempty"`
	RetryDelay *TaskDuration `json:"retry_delay,omitempty"`
	Timeout    *TaskDuration `json:"timeout,omitempty"`
}

type TaskType int
type TaskDuration time.Duration

const (
	TaskTypeShell TaskType = iota
)

func (t TaskType) String() string {
	switch t {
	case TaskTypeShell:
		return "shell"
	default:
		return "unknown"
	}
}

func (t *TaskType) ParseTaskType(s string) error {
	switch s {
	case "shell":
		*t = TaskTypeShell
	default:
		return fmt.Errorf("invalid task type: %s", s)
	}
	return nil
}

func (t *TaskType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	switch s {
	case "shell":
		*t = TaskTypeShell
	default:
		return fmt.Errorf("unknown task type: %s", s)
	}
	return nil
}

func (t TaskType) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (d *TaskDuration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	duration, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("failed to parse duration: %w", err)
	}

	*d = TaskDuration(duration)
	return nil
}

func (d TaskDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d TaskDuration) String() string {
	return time.Duration(d).String()
}
