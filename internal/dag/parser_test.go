package dag

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidDag(t *testing.T) {
	expectedTime, _ := time.Parse(time.RFC3339, "2025-07-11T14:00:00Z")
	testRetry := 3
	testRetryDelay := TaskDuration(15 * time.Second)
	testTimeout := TaskDuration(5 * time.Minute)

	tests := []struct {
		name     string
		config   string
		expected Dag
	}{
		{
			name:   "Valid 1",
			config: `{"dag_id":"dag-1","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":[]}]}`,
			expected: Dag{
				Id:            "dag-1",
				Description:   "description",
				Schedule:      "0 * * * *",
				StartDate:     expectedTime,
				MaxActiveRuns: 1,
				DefaultArgs: &TaskArgs{
					Retries:    &testRetry,
					RetryDelay: &testRetryDelay,
					TimeOut:    &testTimeout,
				},
				Tasks: []Task{
					{
						TaskId:    "task-1",
						Type:      TaskTypeShell,
						Command:   "echo 'hello'",
						DependsOn: []string{},
					},
				},
			},
		},
		{
			name:   "Valid 2",
			config: `{"dag_id":"dag-2","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: Dag{
				Id:            "dag-2",
				Description:   "description",
				Schedule:      "0 * * * *",
				StartDate:     expectedTime,
				MaxActiveRuns: 1,
				DefaultArgs: &TaskArgs{
					Retries:    &testRetry,
					RetryDelay: &testRetryDelay,
					TimeOut:    &testTimeout,
				},
				Tasks: []Task{
					{
						TaskId:    "task-1",
						Type:      TaskTypeShell,
						Command:   "echo 'hello'",
						DependsOn: []string{"task-2"},
					},
					{
						TaskId:    "task-2",
						Type:      TaskTypeShell,
						Command:   "echo 'hello 2'",
						DependsOn: []string{},
					},
				},
			},
		},
		{
			name:   "No optional description",
			config: `{"dag_id":"dag-3","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: Dag{
				Id:            "dag-3",
				Description:   "",
				Schedule:      "0 * * * *",
				StartDate:     expectedTime,
				MaxActiveRuns: 1,
				DefaultArgs: &TaskArgs{
					Retries:    &testRetry,
					RetryDelay: &testRetryDelay,
					TimeOut:    &testTimeout,
				},
				Tasks: []Task{
					{
						TaskId:    "task-1",
						Type:      TaskTypeShell,
						Command:   "echo 'hello'",
						DependsOn: []string{"task-2"},
					},
					{
						TaskId:    "task-2",
						Type:      TaskTypeShell,
						Command:   "echo 'hello 2'",
						DependsOn: []string{},
					},
				},
			},
		},
		{
			name:   "No optional default_args",
			config: `{"dag_id":"dag-4","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: Dag{
				Id:            "dag-4",
				Description:   "description",
				Schedule:      "0 * * * *",
				StartDate:     expectedTime,
				MaxActiveRuns: 1,
				DefaultArgs:   nil,
				Tasks: []Task{
					{
						TaskId:    "task-1",
						Type:      TaskTypeShell,
						Command:   "echo 'hello'",
						DependsOn: []string{"task-2"},
					},
					{
						TaskId:    "task-2",
						Type:      TaskTypeShell,
						Command:   "echo 'hello 2'",
						DependsOn: []string{},
					},
				},
			},
		},
		{
			name:   "No optional retry in default_args",
			config: `{"dag_id":"dag-5","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: Dag{
				Id:            "dag-5",
				Description:   "description",
				Schedule:      "0 * * * *",
				StartDate:     expectedTime,
				MaxActiveRuns: 1,
				DefaultArgs: &TaskArgs{
					Retries:    nil,
					RetryDelay: &testRetryDelay,
					TimeOut:    &testTimeout,
				},
				Tasks: []Task{
					{
						TaskId:    "task-1",
						Type:      TaskTypeShell,
						Command:   "echo 'hello'",
						DependsOn: []string{"task-2"},
					},
					{
						TaskId:    "task-2",
						Type:      TaskTypeShell,
						Command:   "echo 'hello 2'",
						DependsOn: []string{},
					},
				},
			},
		},
		{
			name:   "No optional retry_delay in default_args",
			config: `{"dag_id":"dag-6","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: Dag{
				Id:            "dag-6",
				Description:   "description",
				Schedule:      "0 * * * *",
				StartDate:     expectedTime,
				MaxActiveRuns: 1,
				DefaultArgs: &TaskArgs{
					Retries:    &testRetry,
					RetryDelay: nil,
					TimeOut:    &testTimeout,
				},
				Tasks: []Task{
					{
						TaskId:    "task-1",
						Type:      TaskTypeShell,
						Command:   "echo 'hello'",
						DependsOn: []string{"task-2"},
					},
					{
						TaskId:    "task-2",
						Type:      TaskTypeShell,
						Command:   "echo 'hello 2'",
						DependsOn: []string{},
					},
				},
			},
		},
		{
			name:   "No optional timeout in default_args",
			config: `{"dag_id":"dag-7","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: Dag{
				Id:            "dag-7",
				Description:   "description",
				Schedule:      "0 * * * *",
				StartDate:     expectedTime,
				MaxActiveRuns: 1,
				DefaultArgs: &TaskArgs{
					Retries:    &testRetry,
					RetryDelay: &testRetryDelay,
					TimeOut:    nil,
				},
				Tasks: []Task{
					{
						TaskId:    "task-1",
						Type:      TaskTypeShell,
						Command:   "echo 'hello'",
						DependsOn: []string{"task-2"},
					},
					{
						TaskId:    "task-2",
						Type:      TaskTypeShell,
						Command:   "echo 'hello 2'",
						DependsOn: []string{},
					},
				},
			},
		},
	}

	parser := NewJSONParser()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader(tc.config)

			result, err := parser.Parse(reader)
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, tc.expected, *result)
		})
	}
}

func TestInvalidDag(t *testing.T) {
	tests := []struct {
		name     string
		config   string
		expected string
	}{
		{
			name:     "Missing Id",
			config:   `{"description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":[]}]}`,
			expected: "invalid dag config: dag_id is required",
		},
		{
			name:     "Missing schedule",
			config:   `{"dag_id":"dag-3","description":"description","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: "invalid dag config: cron schedule is required",
		},
		{
			name:     "Missing start_date",
			config:   `{"dag_id":"dag-4","description":"description","schedule":"0 * * * *","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":["task-2"]},{"task_id":"task-2","type":"shell","command":"echo 'hello 2'","depends_on":[]}]}`,
			expected: "invalid dag config: start_date is required",
		},
		{
			name:     "Missing max_active_runs",
			config:   `{"dag_id":"dag-5","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"},"tasks":[{"task_id":"task-1","type":"shell","command":"echo 'hello'","depends_on":[]}]}`,
			expected: "invalid dag config: max_active_runs must be at least 1",
		},
		{
			name:     "Missing tasks",
			expected: "invalid dag config: tasks is required",
			config:   `{"dag_id":"dag-1","description":"description","schedule":"0 * * * *","start_date":"2025-07-11T14:00:00Z","max_active_runs":1,"default_args":{"retries":3,"retry_delay":"15s","timeout":"5m"}}`,
		},
	}
	parser := NewJSONParser()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := strings.NewReader(tc.config)

			_, err := parser.Parse(reader)
			if err == nil {
				t.Error("expected error but got nil")
			} else {
				assert.Equal(t, tc.expected, err.Error())
			}
		})
	}
}
