package dag

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type Parser interface {
	Parse(reader io.Reader) (*Dag, error)
}

type JSONParser struct{}

func NewJSONParser() *JSONParser {
	return &JSONParser{}
}

func (p *JSONParser) Parse(reader io.Reader) (*Dag, error) {
	var dag Dag

	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(&dag); err != nil {
		return nil, fmt.Errorf("failed to parse json: %w", err)
	}

	validator := NewValidator()
	if err := validator.Validate(&dag); err != nil {
		return nil, fmt.Errorf("invalid dag config: %w", err)
	}

	p.applyDefaults(&dag)

	return &dag, nil
}

func (p *JSONParser) applyDefaults(dag *Dag) {
	const (
		defaultRetries    = 0
		defaultRetryDelay = 5 * time.Second
		defaultTimeout    = 60 * time.Second
	)

	for i := range dag.Tasks {
		task := &dag.Tasks[i]

		if task.Retries == nil {
			if dag.DefaultArgs != nil && dag.DefaultArgs.Retries != nil {
				retriesCopy := *dag.DefaultArgs.Retries
				task.Retries = &retriesCopy
			} else {
				task.Retries = new(int)
				*task.Retries = defaultRetries
			}
		}

		if task.RetryDelay == nil {
			if dag.DefaultArgs != nil && dag.DefaultArgs.RetryDelay != nil {
				delayCopy := *dag.DefaultArgs.RetryDelay
				task.RetryDelay = &delayCopy
			} else {
				task.RetryDelay = new(TaskDuration)
				*task.RetryDelay = TaskDuration(defaultRetryDelay)
			}
		}

		if task.Timeout == nil {
			if dag.DefaultArgs != nil && dag.DefaultArgs.Timeout != nil {
				timeoutCopy := *dag.DefaultArgs.Timeout
				task.Timeout = &timeoutCopy
			} else {
				task.Timeout = new(TaskDuration)
				*task.Timeout = TaskDuration(defaultTimeout)
			}
		}
	}
}
