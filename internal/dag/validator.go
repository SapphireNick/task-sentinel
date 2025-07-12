package dag

import "errors"

type Validator struct{}

func NewValidator() *Validator {
	return &Validator{}
}

func (v *Validator) Validate(dag *Dag) error {
	// StartDate, RetryDelay, TimeOut and Type get validated on Unmarshall
	if dag.Id == "" {
		return errors.New("dag_id is required")
	}

	if dag.Schedule == "" {
		return errors.New("cron schedule is required")
	}

	if dag.StartDate.IsZero() {
		return errors.New("start_date is required")
	}

	if dag.MaxActiveRuns < 1 {
		return errors.New("max_active_runs must be at least 1")
	}

	if len(dag.Tasks) == 0 {
		return errors.New("tasks is required")
	}

	if err := v.checkForCircularDependency(&dag.Tasks); err != nil {
		return err
	}

	for _, task := range dag.Tasks {
		if err := v.validateTask(&task); err != nil {
			return err
		}
	}

	return nil
}

func (v *Validator) validateTask(task *Task) error {
	if task.TaskId == "" {
		return errors.New("task_id in task is required")
	}

	if task.Command == "" {
		return errors.New("command in task is required")
	}

	if task.DependsOn == nil {
		return errors.New("depends_on in task is required")
	}

	if task.Retries != nil && *task.Retries < 0 {
		return errors.New("retries in task can not be a negative number")
	}

	if task.RetryDelay != nil && *task.RetryDelay < 0 {
		return errors.New("retry_delay in task can not be a negative number")
	}

	if task.Timeout != nil && *task.Timeout <= 0 {
		return errors.New("timeout in task must be greater than 0")
	}

	return nil
}

func (v *Validator) checkForCircularDependency(tasks *[]Task) error {
	graph := make(map[string][]string)
	for _, task := range *tasks {
		graph[task.TaskId] = task.DependsOn
	}

	visited := make(map[string]bool, len(*tasks))
	onStack := make(map[string]bool, len(*tasks))
	parent := make(map[string]string)

	getCycle := func(task string) error {
		cycleMessage := "found circular dependency: ["
		for parent[task] != task {
			cycleMessage += task + "->"
			task = parent[task]
		}
		return errors.New(cycleMessage)
	}

	var dfs func(node string) error
	dfs = func(node string) error {
		if visited[node] {
			return getCycle(node)
		}
		if onStack[node] {
			return nil
		}

		visited[node] = true
		onStack[node] = true

		for _, dep := range graph[node] {
			parent[dep] = node
			if err := dfs(dep); err != nil {
				return err
			}
		}

		onStack[node] = false
		return nil
	}

	for _, task := range *tasks {
		if !visited[task.TaskId] {
			if err := dfs(task.TaskId); err != nil {
				return err
			}
		}
	}

	return nil
}
