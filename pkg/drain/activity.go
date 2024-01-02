// Package drain migrates running incompatible Temporal workflows
// to new task queues through versioning and ContinueAsNew
package drain

import (
	"context"
	"fmt"
	"strings"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	temporal "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const (
	// ContinueAsNewSignal is sent to temporal client's SignalWorkflow
	ContinueAsNewSignal = "continue-as-new-signal"

	versionedFilter = "'versioned:%s'"
	buildIDFilter   = `
	 AND (
		BuildIds IN (%s)
		 OR BuildIds IS NULL
	)`
	versionQuery = `
		TaskQueue='%s'
		 AND WorkflowType='%s'
		 AND ExecutionStatus='Running'
	`
	terminateReason           = "workflow failed to ContinueAsNew"
	workflowCompleted         = "workflow execution already completed"
	versionExistsErrFormatter = "version %s already exists"
)

// CurrentExecutions creates a query to find running workflows
// and filters by old BuildIds so QueueDrainWorkflow is idempotent
func (c Client) CurrentExecutions(
	ctx context.Context, in CurrentExecutionIn,
) (CurrentExecutionOut, error) {
	query, err := c.getWorkflowQuery(ctx, in.Version, in.Queue, in.WorkflowType)
	if err != nil {
		return CurrentExecutionOut{}, err
	}

	list, err := c.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: c.namespace,
		Query:     query,
	})
	if err != nil {
		return CurrentExecutionOut{}, err
	}

	return getNonEmptyExecutions(list)
}

func getNonEmptyExecutions(
	list *workflowservice.ListWorkflowExecutionsResponse,
) (CurrentExecutionOut, error) {
	if list == nil || list.Executions == nil {
		return CurrentExecutionOut{}, ErrNilResponse
	}

	executions := []workflow.Execution{}
	for _, execution := range list.Executions {
		if execution != nil && execution.Execution != nil {
			executions = append(executions, workflow.Execution{
				ID:    execution.Execution.WorkflowId,
				RunID: execution.Execution.RunId,
			})
		}
	}

	return CurrentExecutionOut{
		Executions: executions,
	}, nil
}

func (c Client) getWorkflowQuery(
	ctx context.Context, version, queue, wfType string,
) (string, error) {
	resp, err := c.temporal.WorkflowService().
		GetWorkerBuildIdCompatibility(
			ctx, &workflowservice.GetWorkerBuildIdCompatibilityRequest{
				Namespace: c.namespace,
				TaskQueue: queue,
			},
		)
	if err != nil {
		return "", err
	}

	return workflowQueryFromBuildIDs(resp, version, queue, wfType)
}

func workflowQueryFromBuildIDs(
	resp *workflowservice.GetWorkerBuildIdCompatibilityResponse, version, queue, wfType string,
) (string, error) {
	if resp == nil {
		return "", ErrNilResponse
	}

	oldBuildIDs := []string{}
	for _, set := range resp.GetMajorVersionSets() {
		for _, buildID := range set.GetBuildIds() {
			if buildID != version {
				oldBuildIDs = append(oldBuildIDs, fmt.Sprintf(versionedFilter, buildID))
			}
		}
	}

	query := fmt.Sprintf(versionQuery, queue, wfType)
	if len(oldBuildIDs) != 0 {
		query += fmt.Sprintf(buildIDFilter, strings.Join(oldBuildIDs, ", "))
	}
	return query, nil
}

// UpgradeBuildCompatibility registers the version as default
// in preparation for migrating all "legacy" workflow executions
func (c Client) UpgradeBuildCompatibility(ctx context.Context, in UpgradeIn) error {
	if err := c.temporal.UpdateWorkerBuildIdCompatibility(
		ctx, newDefaultCompatibilityOption(in),
	); err == nil || err.Error() != fmt.Sprintf(versionExistsErrFormatter, in.Version) {
		return err
	}

	return c.temporal.UpdateWorkerBuildIdCompatibility(ctx, promoteCompatibilityOption(in))
}

func newDefaultCompatibilityOption(in UpgradeIn) *temporal.UpdateWorkerBuildIdCompatibilityOptions {
	return &temporal.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: in.Queue,
		Operation: &temporal.BuildIDOpAddNewIDInNewDefaultSet{
			BuildID: in.Version,
		},
	}
}

func promoteCompatibilityOption(in UpgradeIn) *temporal.UpdateWorkerBuildIdCompatibilityOptions {
	return &temporal.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: in.Queue,
		Operation: &temporal.BuildIDOpPromoteSet{
			BuildID: in.Version,
		},
	}
}

// GetStatus finds the status code of a workflow
func (c Client) GetStatus(ctx context.Context, execution workflow.Execution) (StatusOut, error) {
	return handleDescribeExecutionStatus(
		c.temporal.DescribeWorkflowExecution(ctx, execution.ID, execution.RunID),
	)
}

func handleDescribeExecutionStatus(
	resp *workflowservice.DescribeWorkflowExecutionResponse, err error,
) (StatusOut, error) {
	if err != nil {
		return StatusOut{}, err
	}

	if resp == nil {
		return StatusOut{}, ErrNilResponse
	}

	status := resp.WorkflowExecutionInfo.GetStatus()
	return StatusOut{
		Name:   enums.WorkflowExecutionStatus_name[int32(status)],
		Status: status,
	}, nil
}

// ContinueAsNew sends a signal to a running workflow to continue
func (c Client) ContinueAsNew(ctx context.Context, execution workflow.Execution) error {
	return c.temporal.SignalWorkflow(
		ctx, execution.ID, execution.RunID, ContinueAsNewSignal, nil,
	)
}

// TerminateWorkflow uses wfID and runID to terminate a running workflow
// and catches an error if the workflow previously completed
func (c Client) TerminateWorkflow(ctx context.Context, execution workflow.Execution) error {
	return handleTerminationError(
		c.temporal.TerminateWorkflow(ctx, execution.ID, execution.RunID, terminateReason),
	)
}

func handleTerminationError(err error) error {
	if err == nil || err.Error() == workflowCompleted {
		return nil
	}

	return err
}
