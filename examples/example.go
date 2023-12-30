// Package examples shows the requirements of a workflow before running QueueDrainWorkflow
package examples

import (
	"errors"
	"time"

	"github.com/kevindweb/version-drain/pkg/drain"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func activityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{},
	}
}

type wfState struct {
	checkpoint int
}

// ExampleContinueWorkflow does work with a selector
// while waiting for a ContinueAsNwSignal
func ExampleContinueWorkflow(ctx workflow.Context, state wfState) error {
	ctx = workflow.WithActivityOptions(ctx, activityOptions())
	var cas error
	continueAsNew := workflow.GetSignalChannel(ctx, drain.ContinueAsNewSignal)
	selector := workflow.NewSelector(ctx).
		AddReceive(continueAsNew, func(c workflow.ReceiveChannel, more bool) {
			cas = errors.New("continue")
		})

	exitCondition := false
	log := workflow.GetLogger(ctx)
	for {
		selector.Select(ctx)
		if cas != nil {
			// remember to drain other signals
			return exitContinueAsNew(ctx, state)
		}

		// your businss logic here
		if exitCondition {
			return nil
		}

		state.checkpoint += 1
		log.Debug("no error yet")
	}
}

// exitContinueAsNew represents the most critical part of the entire workflow
// as we must tell Temporal that we need to be continued on the newest task queue version
// which ensures our "old" worker does not pick up this task
func exitContinueAsNew(ctx workflow.Context, state wfState) error {
	ctx = workflow.WithWorkflowVersioningIntent(ctx, temporal.VersioningIntentDefault)
	return workflow.NewContinueAsNewError(ctx, ExampleContinueWorkflow, state)
}
