// Package main shows an example Temporal worker registering the drain workflow options
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kevindweb/version-drain/pkg/drain"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const (
	// compatibilityVersion represents the version your team will "bump"
	// to keep Temporal clients and workers assigning work on the correct queue
	compatibilityVersion = "1.0"
)

func newClient() client.Client {
	c, err := client.NewLazyClient(client.Options{})
	if err != nil {
		panic(err)
	}
	return c
}

func setup() {
	c := newClient()
	queue := "example-queue"
	w := worker.New(c, queue, options())
	config := drain.Config{
		Temporal:  c,
		Namespace: "example-ns",
	}
	if err := drain.Register(w, config); err != nil {
		panic(err)
	}
}

func options() worker.Options {
	return worker.Options{
		BuildID:                 compatibilityVersion,
		UseBuildIDForVersioning: true,
	}
}

func versionBump(version, queue string) {
	wf := "ExampleContinueWorkflow"
	workflowOptions := client.StartWorkflowOptions{
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		ID:                    fmt.Sprintf("migrate-%s-%s", wf, version),
		TaskQueue:             queue,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	init := drain.VersionDrainIn{
		Queue:        queue,
		WorkflowType: wf,
		Version:      version,
	}
	if _, err := newClient().ExecuteWorkflow(
		ctx, workflowOptions, drain.QueueDrainWorkflow, init,
	); err != nil {
		panic(err)
	}
}

func main() {
	setup()
}
