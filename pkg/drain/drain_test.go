package drain_test

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kevindweb/version-drain/pkg/drain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const (
	anything = mock.Anything
)

var (
	defaultValidInput = drain.VersionDrainIn{
		Queue:        "test-queue",
		Version:      "1.4",
		WorkflowType: "BillingWorkflow",
	}
)

func TestBasicDrain(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name              string
		in                drain.VersionDrainIn
		executions        drain.CurrentExecutionOut
		continuanceStatus drain.ContinuanceStatus
		wantErr           bool
		want              drain.VersionDrainResults
	}{
		{
			name:    "invalid input",
			in:      drain.VersionDrainIn{},
			wantErr: true,
		},
		{
			name: "no executions found",
			in:   defaultValidInput,
			want: drain.VersionDrainResults{
				Workflows: []drain.ContinuanceStatus{},
			},
		},
		{
			name: "one continued workflow",
			in:   defaultValidInput,
			executions: drain.CurrentExecutionOut{
				Executions: []workflow.Execution{{}},
			},
			continuanceStatus: drain.ContinuanceStatus{
				Execution: workflow.Execution{},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			},
			want: drain.VersionDrainResults{
				Workflows: []drain.ContinuanceStatus{
					{
						Execution: workflow.Execution{},
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
					},
				},
			},
		},
		{
			name: "multiple workflows same ID error",
			in:   defaultValidInput,
			executions: drain.CurrentExecutionOut{
				Executions: []workflow.Execution{
					{
						ID:    "id1",
						RunID: "run1",
					}, {
						ID:    "id1",
						RunID: "run1",
					},
				},
			},
			wantErr: true,
			continuanceStatus: drain.ContinuanceStatus{
				Execution: workflow.Execution{},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			},
		},
		{
			name: "multiple failed workflows",
			in:   defaultValidInput,
			executions: drain.CurrentExecutionOut{
				Executions: []workflow.Execution{
					{
						ID:    "id1",
						RunID: "run1",
					}, {
						ID:    "id2",
						RunID: "run2",
					}, {
						ID:    "id3",
						RunID: "run3",
					}, {
						ID:    "id4",
						RunID: "run4",
					},
				},
			},
			continuanceStatus: drain.ContinuanceStatus{
				Execution: workflow.Execution{},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			},
			want: drain.VersionDrainResults{
				Workflows: []drain.ContinuanceStatus{
					{
						Execution: workflow.Execution{},
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
					},
					{
						Execution: workflow.Execution{},
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
					},
					{
						Execution: workflow.Execution{},
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
					},
					{
						Execution: workflow.Execution{},
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
					},
				},
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var (
				err       error
				testSuite = &testsuite.WorkflowTestSuite{}
				env       = testSuite.NewTestWorkflowEnvironment()
			)

			env.SetWorkflowRunTimeout(1 * time.Hour)
			var c drain.Client
			env.OnActivity(c.UpgradeBuildCompatibility, anything, anything).
				Return(nil)
			env.OnActivity(c.CurrentExecutions, anything, anything).
				Return(tt.executions, nil)
			env.OnWorkflow(drain.ContinuanceWorkflow, anything, anything).
				Return(tt.continuanceStatus, nil)

			env.ExecuteWorkflow(drain.QueueDrainWorkflow, tt.in)
			require.True(t, env.IsWorkflowCompleted())
			err = env.GetWorkflowError()

			if tt.wantErr && err == nil {
				t.Fatalf("wanted error but none received")
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if tt.wantErr && err != nil {
				return
			}

			var result drain.VersionDrainResults
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, tt.want, result)
		})
	}
}

func TestDrainInput(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		in      drain.VersionDrainIn
		wantErr bool
	}{
		{
			name: "empty workflow type",
			in: drain.VersionDrainIn{
				Queue:        "test-queue",
				Version:      "1.4",
				WorkflowType: "",
			},
			wantErr: true,
		},
		{
			name: "empty queue",
			in: drain.VersionDrainIn{
				Queue:        "",
				Version:      "1.4",
				WorkflowType: "BillingWorkflow",
			},
			wantErr: true,
		},
		{
			name: "empty version",
			in: drain.VersionDrainIn{
				Queue:        "test-queue",
				Version:      "",
				WorkflowType: "BillingWorkflow",
			},
			wantErr: true,
		},
		{
			name: "valid input",
			in:   defaultValidInput,
		},
	} {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.in.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("wanted error but none received")
			}

			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if tt.wantErr && err != nil {
				return
			}
		})
	}
}

func TestSleepPoll(t *testing.T) {
	t.Parallel()
	exampleErr := errors.New("example")
	for _, tt := range []struct {
		name      string
		statuses  []enumspb.WorkflowExecutionStatus
		errors    []error
		wait      time.Duration
		execution workflow.Execution
		want      enumspb.WorkflowExecutionStatus
		wantErr   bool
	}{
		{
			name:   "timed out after 1 poll and wf still running",
			wait:   drain.ContinueAsNewPollInterval,
			errors: []error{nil},
			statuses: []enumspb.WorkflowExecutionStatus{
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			want: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		{
			name:   "error after one poll",
			wait:   drain.ContinueAsNewPollInterval * 4,
			errors: []error{nil, exampleErr},
			statuses: []enumspb.WorkflowExecutionStatus{
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			wantErr: true,
		},
		{
			name:   "workflow failed after 2 polls",
			wait:   drain.ContinueAsNewPollInterval * 4,
			errors: []error{nil, nil, nil},
			statuses: []enumspb.WorkflowExecutionStatus{
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			},
			want: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var (
				result    enumspb.WorkflowExecutionStatus
				d         drain.Client
				testSuite = &testsuite.WorkflowTestSuite{}
				env       = testSuite.NewTestWorkflowEnvironment()
				calls     = 0
			)

			env.SetWorkflowRunTimeout(1 * time.Hour)
			env.OnActivity(d.GetStatus, anything, anything).
				Return(func(
					ctx context.Context, execution workflow.Execution,
				) (drain.StatusOut, error) {
					status := tt.statuses[calls]
					err := tt.errors[calls]
					if err != nil {
						return drain.StatusOut{}, temporal.NewApplicationError("err", "err", err)
					}
					calls++
					return drain.StatusOut{
						Status: status,
					}, err
				})

			env.ExecuteWorkflow(
				func(ctx workflow.Context) (enumspb.WorkflowExecutionStatus, error) {
					ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: time.Second * 30,
					})
					return drain.SleepUntilContinued(ctx, tt.execution, tt.wait)
				},
			)
			require.True(t, env.IsWorkflowCompleted())
			require.Equal(t, tt.wantErr, env.GetWorkflowError() != nil)
			if tt.wantErr {
				return
			}
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, tt.want, result)
		})
	}
}

type testWorker struct {
	registeredActivities map[string]bool
}

func (*testWorker) Start() error { return nil }
func (*testWorker) Run(
	_ <-chan interface{},
) error {
	return nil
}
func (*testWorker) Stop()                                                                 {}
func (*testWorker) RegisterWorkflow(_ interface{})                                        {}
func (*testWorker) RegisterWorkflowWithOptions(_ interface{}, _ workflow.RegisterOptions) {}
func (t *testWorker) RegisterActivity(i interface{}) {
	if t.registeredActivities == nil {
		t.registeredActivities = map[string]bool{}
	}
	t.registeredActivities[getMethodName(i)] = true
}

func getMethodName(iface interface{}) string {
	val := reflect.ValueOf(iface)
	if val.Kind() != reflect.Func {
		return ""
	}

	methodName := runtime.FuncForPC(val.Pointer()).Name()
	lastDot := len(methodName) - 1
	for i := lastDot; i >= 0; i-- {
		if methodName[i] == '.' {
			methodName = methodName[i+1:]
			break
		}
	}

	return strings.TrimSuffix(methodName, "-fm")
}
func (*testWorker) RegisterActivityWithOptions(_ interface{}, _ activity.RegisterOptions) {}

func TestClientRegister(t *testing.T) {
	t.Parallel()
	c, err := client.NewLazyClient(client.Options{})
	require.NoError(t, err)
	for _, tt := range []struct {
		name    string
		config  drain.Config
		wantErr error
	}{
		{
			name:    "temporal client nil",
			wantErr: drain.ErrNilClient,
		},
		{
			name:    "temporal namespace empty",
			wantErr: drain.ErrEmptyNamespace,
			config: drain.Config{
				Temporal: c,
			},
		},
		{
			name: "valid config checks activity registration",
			config: drain.Config{
				Temporal:  c,
				Namespace: "test",
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			worker := &testWorker{}
			require.Equal(t, tt.wantErr, drain.Register(worker, tt.config))
			if tt.wantErr != nil {
				return
			}
			methodMap := inspectStructMethods(drain.Client{})
			registeredMap := worker.registeredActivities
			require.Equal(
				t, methodMap, registeredMap,
				"workflow does not register all public methods",
			)
		})
	}
}

func inspectStructMethods(s interface{}) map[string]bool {
	val := reflect.ValueOf(s)
	if val.Kind() != reflect.Struct {
		return nil
	}

	expectedMethods := map[string]bool{}
	for i := 0; i < val.NumMethod(); i++ {
		method := val.Type().Method(i)
		if method.IsExported() {
			firstArgCtx := method.Type.In(1) == reflect.TypeOf((*context.Context)(nil)).Elem()
			if firstArgCtx {
				expectedMethods[method.Name] = true
			}
		}
	}
	return expectedMethods
}
