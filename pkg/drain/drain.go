package drain

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

const (
	// ContinueAsNewWaitTime is the time we wait polling for ContinueAsNew success
	ContinueAsNewWaitTime = time.Minute * 3
	// ContinueAsNewPollInterval defines our sleep cycle waiting for workflow status
	ContinueAsNewPollInterval = time.Second * 1

	continuanceFormatter = "continue-%s-%s"
)

// QueueDrainWorkflow creates a new version in a task queue and migrates all
// running workflows of a given type over after waiting for ContinueAsNew
func QueueDrainWorkflow(ctx workflow.Context, in VersionDrainIn) (VersionDrainResults, error) {
	if err := in.Validate(); err != nil {
		return VersionDrainResults{}, err
	}

	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	if err := upgrade(ctx, in); err != nil {
		return VersionDrainResults{}, err
	}

	queryData := CurrentExecutionIn{
		Queue:        in.Queue,
		Version:      in.Version,
		WorkflowType: in.WorkflowType,
	}
	var results CurrentExecutionOut
	if err := workflow.ExecuteActivity(
		ctx, (Client).CurrentExecutions, queryData,
	).Get(ctx, &results); err != nil {
		return VersionDrainResults{}, err
	}

	waitTime := in.WaitTime
	if waitTime == 0 {
		waitTime = ContinueAsNewWaitTime
	}

	futures := []workflow.Future{}
	for _, execution := range results.Executions {
		continueIn := ContinuanceIn{
			Execution: execution,
			WaitTime:  waitTime,
		}
		cwx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: fmt.Sprintf(continuanceFormatter, execution.ID, in.Version),
		})
		futures = append(futures, workflow.ExecuteChildWorkflow(
			cwx, ContinuanceWorkflow, continueIn,
		))
	}

	statuses, err := workflowStatuses(ctx, futures)
	if err != nil {
		return VersionDrainResults{}, err
	}

	return VersionDrainResults{
		Workflows: statuses,
	}, nil
}

func workflowStatuses(
	ctx workflow.Context, futures []workflow.Future,
) ([]ContinuanceStatus, error) {
	statuses := []ContinuanceStatus{}
	for _, child := range futures {
		var status ContinuanceStatus
		if err := child.Get(ctx, &status); err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func upgrade(ctx workflow.Context, in VersionDrainIn) error {
	return workflow.ExecuteActivity(
		ctx, (Client).UpgradeBuildCompatibility, UpgradeIn{
			Queue:   in.Queue,
			Version: in.Version,
		},
	).Get(ctx, nil)
}

// ContinuanceWorkflow continues a single wf onto the new
// versioned queue or terminates if polling times out
func ContinuanceWorkflow(ctx workflow.Context, in ContinuanceIn) (ContinuanceStatus, error) {
	execution := in.Execution
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	if err := workflow.ExecuteActivity(
		ctx, (Client).ContinueAsNew, execution,
	).Get(ctx, nil); err != nil {
		return ContinuanceStatus{}, err
	}

	if status, err := SleepUntilContinued(ctx, execution, in.WaitTime); err != nil {
		return ContinuanceStatus{}, err
	} else if status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return ContinuanceStatus{
			Execution: execution,
			Status:    status,
		}, nil
	}

	if err := workflow.ExecuteActivity(
		ctx, (Client).TerminateWorkflow, execution,
	).Get(ctx, nil); err != nil {
		return ContinuanceStatus{}, err
	}

	var res StatusOut
	if err := workflow.ExecuteActivity(
		ctx, (Client).GetStatus, execution,
	).Get(ctx, &res); err != nil {
		return ContinuanceStatus{}, err
	}

	return ContinuanceStatus{
		Execution: execution,
		Status:    res.Status,
	}, nil
}

// SleepUntilContinued polls a workflow's status while it is running
func SleepUntilContinued(
	ctx workflow.Context, execution workflow.Execution, waitTime time.Duration,
) (enumspb.WorkflowExecutionStatus, error) {
	var pollTime = time.Second * 0
	for pollTime < waitTime {
		var res StatusOut
		if err := workflow.ExecuteActivity(
			ctx, (Client).GetStatus, execution,
		).Get(ctx, &res); err != nil {
			return enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, err
		}

		if res.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return res.Status, nil
		}

		pollTime += ContinueAsNewPollInterval
		if err := workflow.Sleep(ctx, ContinueAsNewPollInterval); err != nil {
			return enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, err
		}
	}

	return enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, nil
}
