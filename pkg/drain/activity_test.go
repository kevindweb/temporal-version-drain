package drain

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	common "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	wfapi "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	temporal "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

func TestNonEmptyExecutionList(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		list      *workflowservice.ListWorkflowExecutionsResponse
		expectOut CurrentExecutionOut
		expectErr error
	}{
		{
			name:      "nil response list",
			list:      nil,
			expectErr: ErrNilResponse,
		},
		{
			name: "no executions found",
			list: &workflowservice.ListWorkflowExecutionsResponse{
				Executions: []*wfapi.WorkflowExecutionInfo{},
			},
			expectOut: CurrentExecutionOut{
				Executions: []workflow.Execution{},
			},
		},
		{
			name: "nil executions excluded",
			list: &workflowservice.ListWorkflowExecutionsResponse{
				Executions: []*wfapi.WorkflowExecutionInfo{
					nil,
					{
						Execution: &common.WorkflowExecution{
							WorkflowId: "wf2",
							RunId:      "run2",
						},
					},
					{
						Execution: &common.WorkflowExecution{
							WorkflowId: "wf1",
							RunId:      "run1",
						},
					},
					{
						Execution: nil,
					},
				},
			},
			expectOut: CurrentExecutionOut{
				Executions: []workflow.Execution{
					{
						ID:    "wf2",
						RunID: "run2",
					},
					{
						ID:    "wf1",
						RunID: "run1",
					},
				},
			},
		},
	} {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := getNonEmptyExecutions(tt.list)
			require.Equal(t, tt.expectErr, err)
			if tt.expectErr != nil {
				return
			}
			require.Equal(t, tt.expectOut, out)
		})
	}
}

func TestBuildIDQuery(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name        string
		resp        *workflowservice.GetWorkerBuildIdCompatibilityResponse
		version     string
		queue       string
		wfType      string
		expectErr   error
		expectQuery string
	}{
		{
			name: "all old build IDs",
			resp: &workflowservice.GetWorkerBuildIdCompatibilityResponse{
				MajorVersionSets: []*taskqueue.CompatibleVersionSet{
					{
						BuildIds: []string{"3.0", "3.1"},
					},
					{
						BuildIds: []string{"1.0"},
					},
					{
						BuildIds: []string{"2.0"},
					},
				},
			},
			version: "1.0",
			queue:   "queue",
			wfType:  "ExampleContinueWorkflow",
			expectQuery: `
				TaskQueue='queue'
				 AND WorkflowType='ExampleContinueWorkflow'
				 AND ExecutionStatus='Running'
				 AND (
					BuildIds IN ('versioned:3.0', 'versioned:3.1', 'versioned:2.0')
					 OR BuildIds IS NULL
				)
			`,
		},
		{
			name: "version bump no other versions set",
			resp: &workflowservice.GetWorkerBuildIdCompatibilityResponse{
				MajorVersionSets: []*taskqueue.CompatibleVersionSet{
					{
						BuildIds: []string{"1.0"},
					},
				},
			},
			version: "1.0",
			queue:   "queue",
			wfType:  "ExampleContinueWorkflow",
			expectQuery: `
				TaskQueue='queue'
				 AND WorkflowType='ExampleContinueWorkflow'
				 AND ExecutionStatus='Running'
			`,
		},
		{
			name:      "nil temporal response",
			resp:      nil,
			expectErr: ErrNilResponse,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			query, err := workflowQueryFromBuildIDs(tt.resp, tt.version, tt.queue, tt.wfType)
			require.Equal(t, tt.expectErr, err)
			if tt.expectErr != nil {
				return
			}
			expect := strings.ReplaceAll(strings.ReplaceAll(tt.expectQuery, "\n", ""), "\t", "")
			query = strings.ReplaceAll(strings.ReplaceAll(query, "\n", ""), "\t", "")
			require.Equal(t, expect, query)
		})
	}
}

func TestTerminationError(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name   string
		err    error
		expect error
	}{
		{
			name:   "no error",
			err:    nil,
			expect: nil,
		},
		{
			name:   "completed is not an error",
			err:    errors.New(workflowCompleted),
			expect: nil,
		},
		{
			name:   "unknown error",
			err:    errors.New("example"),
			expect: errors.New("example"),
		},
	} {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expect, handleTerminationError(tt.err))
		})
	}
}

const (
	completed = enums.WORKFLOW_EXECUTION_STATUS_COMPLETED
)

func TestHandleExecutionStatus(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name      string
		err       error
		expectErr error
		resp      *workflowservice.DescribeWorkflowExecutionResponse
		expectOut StatusOut
	}{
		{
			name:      "random error",
			err:       errors.New("example"),
			expectErr: errors.New("example"),
		},
		{
			name:      "nil response",
			resp:      nil,
			expectErr: ErrNilResponse,
		},
		{
			name: "completed status",
			resp: &workflowservice.DescribeWorkflowExecutionResponse{
				WorkflowExecutionInfo: &wfapi.WorkflowExecutionInfo{
					Status: completed,
				},
			},
			expectOut: StatusOut{
				Name:   enums.WorkflowExecutionStatus_name[int32(completed)],
				Status: completed,
			},
		},
	} {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := handleDescribeExecutionStatus(tt.resp, tt.err)
			require.Equal(t, tt.expectErr, err)
			if err != nil {
				return
			}
			require.Equal(t, tt.expectOut, out)
		})
	}
}

func TestCompatibilityOptions(t *testing.T) {
	queue := "queue"
	v1 := "1.0"
	in := UpgradeIn{
		Queue:   queue,
		Version: v1,
	}
	opt := newDefaultCompatibilityOption(in)
	expectDefault := &temporal.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: queue,
		Operation: &temporal.BuildIDOpAddNewIDInNewDefaultSet{
			BuildID: v1,
		},
	}
	require.Equal(t, expectDefault, opt)

	opt = promoteCompatibilityOption(in)
	expectPromote := &temporal.UpdateWorkerBuildIdCompatibilityOptions{
		TaskQueue: queue,
		Operation: &temporal.BuildIDOpPromoteSet{
			BuildID: v1,
		},
	}
	require.Equal(t, expectPromote, opt)
}
