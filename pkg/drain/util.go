package drain

import (
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	activityTimeout      = time.Second * 10
	nilStatusResponseMsg = "nil describe status response"
)

var (
	// ErrEmptyQueue validates task queue exists before versioning
	ErrEmptyQueue = errors.New("task queue is empty")
	// ErrEmptyVersion validates version exists before versioning
	ErrEmptyVersion = errors.New("version is empty")
	// ErrEmptyWorkflow validates workflow type exists before versioning
	ErrEmptyWorkflow = errors.New("workflow type is empty")
	// ErrEmptyNamespace validates namespace exists on the activity Client before Register
	ErrEmptyNamespace = errors.New("temporal namespace is empty")
	// ErrNilClient validates the temporal client is not nil before Register
	ErrNilClient = errors.New("temporal client is uninitialized")
	// ErrNilStatusResponse catches the unlikely empty res from DescribeWorkflowExecution
	ErrNilStatusResponse = errors.New(nilStatusResponseMsg)
)

// Client holds temporal client for processing versioning activities
type Client struct {
	temporal  temporalclient.Client
	namespace string
}

// Config holds data to register the drain workflow activities
type Config struct {
	Temporal  temporalclient.Client
	Namespace string
}

func (c Config) validate() error {
	if c.Temporal == nil {
		return ErrNilClient
	}

	if c.Namespace == "" {
		return ErrEmptyNamespace
	}
	return nil
}

// VersionDrainIn holds the input required to run QueueDrainWorkflow
type VersionDrainIn struct {
	Queue        string
	Version      string
	WorkflowType string
	WaitTime     time.Duration
}

// Validate asserts the input for versioning is complete
func (in VersionDrainIn) Validate() error {
	if in.Queue == "" {
		return ErrEmptyQueue
	}

	if in.Version == "" {
		return ErrEmptyVersion
	}

	if in.WorkflowType == "" {
		return ErrEmptyWorkflow
	}

	return nil
}

// VersionDrainResults holds statuses of all updated versioned workflows
type VersionDrainResults struct {
	Workflows []ContinuanceStatus
}

// ContinuanceIn holds the input required to run ContinuanceWorkflow
type ContinuanceIn struct {
	Execution workflow.Execution
	WaitTime  time.Duration
}

// ContinuanceStatus represents a workflow's status after an attempted ContinuanceWorkflow
type ContinuanceStatus struct {
	Execution workflow.Execution
	Status    enumspb.WorkflowExecutionStatus
}

// CurrentExecutionIn holds input required to run Client.CurrentExecutions
type CurrentExecutionIn struct {
	Queue        string
	Version      string
	WorkflowType string
}

// CurrentExecutionOut represents the wfID and runID of all
// running workflows that need to be drained and continued
type CurrentExecutionOut struct {
	Executions []workflow.Execution
}

// StatusOut holds a workflow status enum and human readable status name
type StatusOut struct {
	Name   string
	Status enumspb.WorkflowExecutionStatus
}

// UpgradeIn holds input required to run Client.UpgradeBuildCompatibility
type UpgradeIn struct {
	Queue   string
	Version string
}

func activityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: activityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			NonRetryableErrorTypes: []string{
				nilStatusResponseMsg,
			},
		},
	}
}

// Register should be called when initializing the
// worker that will process verion drain workflows
func Register(w worker.Worker, config Config) error {
	if err := config.validate(); err != nil {
		return err
	}

	c := Client{
		temporal:  config.Temporal,
		namespace: config.Namespace,
	}
	w.RegisterActivity(c.CurrentExecutions)
	w.RegisterActivity(c.UpgradeBuildCompatibility)
	w.RegisterActivity(c.ContinueAsNew)
	w.RegisterActivity(c.GetStatus)
	w.RegisterActivity(c.TerminateWorkflow)
	return nil
}
