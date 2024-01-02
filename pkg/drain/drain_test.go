package drain

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
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
	defaultValidInput = VersionDrainIn{
		Queue:        "test-queue",
		Version:      "1.4",
		WorkflowType: "BillingWorkflow",
	}
)

func TestBasicDrain(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name                string
		in                  VersionDrainIn
		executions          CurrentExecutionOut
		upgradeErr          error
		currentExecutionErr error
		continuanceStatus   ContinuanceStatus
		wantErr             bool
		want                VersionDrainResults
	}{
		{
			name:    "invalid input",
			in:      VersionDrainIn{},
			wantErr: true,
		},
		{
			name: "no executions found",
			in:   defaultValidInput,
			want: VersionDrainResults{
				Workflows: []ContinuanceStatus{},
			},
		},
		{
			name: "one continued workflow",
			in:   defaultValidInput,
			executions: CurrentExecutionOut{
				Executions: []workflow.Execution{{}},
			},
			continuanceStatus: ContinuanceStatus{
				Execution: workflow.Execution{},
				Status:    enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			},
			want: VersionDrainResults{
				Workflows: []ContinuanceStatus{
					{
						Execution: workflow.Execution{},
						Status:    enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
					},
				},
			},
		},
		{
			name:       "compatibility upgrade error",
			in:         defaultValidInput,
			upgradeErr: errors.New("example err"),
			wantErr:    true,
		},
		{
			name:                "current execution error",
			in:                  defaultValidInput,
			currentExecutionErr: errors.New("example err"),
			wantErr:             true,
		},
		{
			name: "multiple workflows same ID error",
			in:   defaultValidInput,
			executions: CurrentExecutionOut{
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
			continuanceStatus: ContinuanceStatus{
				Execution: workflow.Execution{},
				Status:    enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			},
		},
		{
			name: "multiple failed workflows",
			in:   defaultValidInput,
			executions: CurrentExecutionOut{
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
			continuanceStatus: ContinuanceStatus{
				Execution: workflow.Execution{},
				Status:    enums.WORKFLOW_EXECUTION_STATUS_FAILED,
			},
			want: VersionDrainResults{
				Workflows: []ContinuanceStatus{
					{
						Execution: workflow.Execution{},
						Status:    enums.WORKFLOW_EXECUTION_STATUS_FAILED,
					},
					{
						Execution: workflow.Execution{},
						Status:    enums.WORKFLOW_EXECUTION_STATUS_FAILED,
					},
					{
						Execution: workflow.Execution{},
						Status:    enums.WORKFLOW_EXECUTION_STATUS_FAILED,
					},
					{
						Execution: workflow.Execution{},
						Status:    enums.WORKFLOW_EXECUTION_STATUS_FAILED,
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
			var c Client
			env.OnActivity(c.UpgradeBuildCompatibility, anything, anything).
				Return(tt.upgradeErr)
			env.OnActivity(c.CurrentExecutions, anything, anything).
				Return(tt.executions, tt.currentExecutionErr)
			env.OnWorkflow(ContinuanceWorkflow, anything, anything).
				Return(tt.continuanceStatus, nil)

			env.ExecuteWorkflow(QueueDrainWorkflow, tt.in)
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

			var result VersionDrainResults
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, tt.want, result)
		})
	}
}

func TestDrainInput(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		in      VersionDrainIn
		wantErr bool
	}{
		{
			name: "empty workflow type",
			in: VersionDrainIn{
				Queue:        "test-queue",
				Version:      "1.4",
				WorkflowType: "",
			},
			wantErr: true,
		},
		{
			name: "empty queue",
			in: VersionDrainIn{
				Queue:        "",
				Version:      "1.4",
				WorkflowType: "BillingWorkflow",
			},
			wantErr: true,
		},
		{
			name: "empty version",
			in: VersionDrainIn{
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

func TestContinuanceWorkflow(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name         string
		in           ContinuanceIn
		continueErr  error
		statusErr    error
		statusOut    StatusOut
		terminateErr error
		wantErr      bool
		want         ContinuanceStatus
	}{
		{
			name:        "continue as new error",
			continueErr: errors.New("example"),
			wantErr:     true,
		},
		{
			name: "get status error",
			in: ContinuanceIn{
				WaitTime: ContinueAsNewWaitTime,
			},
			statusErr: errors.New("example"),
			wantErr:   true,
		},
		{
			name: "don't terminate on continued",
			in: ContinuanceIn{
				WaitTime: ContinueAsNewWaitTime,
				Execution: workflow.Execution{
					ID:    "id1",
					RunID: "run1",
				},
			},
			statusOut: StatusOut{
				Status: enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			},
			want: ContinuanceStatus{
				Status: enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
				Execution: workflow.Execution{
					ID:    "id1",
					RunID: "run1",
				},
			},
		},
		{
			name: "terminate workflow error",
			in: ContinuanceIn{
				WaitTime: ContinueAsNewWaitTime,
			},
			statusOut: StatusOut{
				Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			terminateErr: errors.New("example"),
			wantErr:      true,
		},
		{
			name: "",
			in: ContinuanceIn{
				WaitTime: ContinueAsNewWaitTime,
				Execution: workflow.Execution{
					ID:    "id1",
					RunID: "run1",
				},
			},
			statusOut: StatusOut{
				Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			want: ContinuanceStatus{
				Status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Execution: workflow.Execution{
					ID:    "id1",
					RunID: "run1",
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
			var c Client
			env.OnActivity(c.ContinueAsNew, anything, anything).
				Return(tt.continueErr)
			env.OnActivity(c.GetStatus, anything, anything).
				Return(tt.statusOut, tt.statusErr)
			env.OnActivity(c.TerminateWorkflow, anything, anything).
				Return(tt.terminateErr)

			env.ExecuteWorkflow(ContinuanceWorkflow, tt.in)
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

			var result ContinuanceStatus
			require.NoError(t, env.GetWorkflowResult(&result))
			require.Equal(t, tt.want, result)
		})
	}
}

func TestSleepPoll(t *testing.T) {
	t.Parallel()
	exampleErr := errors.New("example")
	for _, tt := range []struct {
		name      string
		statuses  []enums.WorkflowExecutionStatus
		errors    []error
		wait      time.Duration
		execution workflow.Execution
		want      enums.WorkflowExecutionStatus
		wantErr   bool
	}{
		{
			name:   "timed out after 1 poll and wf still running",
			wait:   ContinueAsNewPollInterval,
			errors: []error{nil},
			statuses: []enums.WorkflowExecutionStatus{
				enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			want: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		{
			name:   "error after one poll",
			wait:   ContinueAsNewPollInterval * 4,
			errors: []error{nil, exampleErr},
			statuses: []enums.WorkflowExecutionStatus{
				enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
			wantErr: true,
		},
		{
			name:   "workflow failed after 2 polls",
			wait:   ContinueAsNewPollInterval * 4,
			errors: []error{nil, nil, nil},
			statuses: []enums.WorkflowExecutionStatus{
				enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
				enums.WORKFLOW_EXECUTION_STATUS_FAILED,
			},
			want: enums.WORKFLOW_EXECUTION_STATUS_FAILED,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var (
				result    enums.WorkflowExecutionStatus
				d         Client
				testSuite = &testsuite.WorkflowTestSuite{}
				env       = testSuite.NewTestWorkflowEnvironment()
				calls     = 0
			)

			env.SetWorkflowRunTimeout(1 * time.Hour)
			env.OnActivity(d.GetStatus, anything, anything).
				Return(func(
					ctx context.Context, execution workflow.Execution,
				) (StatusOut, error) {
					status := tt.statuses[calls]
					err := tt.errors[calls]
					if err != nil {
						return StatusOut{}, temporal.NewApplicationError("err", "err", err)
					}
					calls++
					return StatusOut{
						Status: status,
					}, err
				})

			env.ExecuteWorkflow(
				func(ctx workflow.Context) (enums.WorkflowExecutionStatus, error) {
					ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: time.Second * 30,
					})
					return SleepUntilContinued(ctx, tt.execution, tt.wait)
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
		config  Config
		wantErr error
	}{
		{
			name:    "temporal client nil",
			wantErr: ErrNilClient,
		},
		{
			name:    "temporal namespace empty",
			wantErr: ErrEmptyNamespace,
			config: Config{
				Temporal: c,
			},
		},
		{
			name: "valid config checks activity registration",
			config: Config{
				Temporal:  c,
				Namespace: "test",
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			worker := &testWorker{}
			require.Equal(t, tt.wantErr, Register(worker, tt.config))
			if tt.wantErr != nil {
				return
			}
			methodMap := inspectStructMethods(Client{})
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
