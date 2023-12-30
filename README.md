# Temporal Version Drain
Version Drain is a workflow for dynamically migrating long-running Temporal workflows to a new versioned task queue to reduce risk and toil from backwards incompatibility.

## Background

### Temporal Versioning
Please read [Temporal worker versioning docs](https://docs.temporal.io/dev-guide/go/versioning#worker-versioning) if you are not already familiar.

### Context
Our team treats developer efficiency and deployment risk as the top priority. To enable quick iteration in Temporal with our month-long workflows, all other solutions were either too risky (`if/else` patching is not comprehensive)
Other solutions we tried had too many failure modes
* [Patching](https://docs.temporal.io/workflows#patching) with`if/else` is not comprehensive and developers make mistakes
* Using `replayer.ReplayWorkflowHistoryFromJSONFile` in CI is great but has race conditions if a new workflow comes after CI passes but before your new code executes
* Versioning entire workflows leaves toilsome cleanup, especially with our rate of CD iteration (10 commits per day)

The Version Drain workflow has been in production successfully for 5 months, performing hundreds of live workflow drains. Developers choose to version the task queue when they want to reduce risk, but since the drain workflow is idempotent, we run without bumping the version to avoid the risk of a race condition (ex. CI goes down for hours during the middle of deployment and new code gets pushed on top of the old).

## QueueDrainWorkflow Logic
1. Set the new compatible build version in the Temporal server
    * `BuildIDOpAddNewIDInNewDefaultSet` for new versions
    * `BuildIDOpPromoteSet` for existing versions (ex. reverting to an old version)
2. Use a query to find the running workflows with a specific `WorkflowType`. Filter out workflows that are already on the new version (maintains idempotency)
3. Execute `ContinuanceWorkflow` to ContinueAsNew all running workflows in parallel
4. Poll checking if the workflow exits with `ContinuedAsNew` status

## Requirements
The following are requirements of your system before invoking `QueueDrainWorkflow`
* `WorkflowType` must be able to receive `ContinueAsNewSignal` and checkpoint itself for continuing
* The drain workflow must be called separately for each WorkflowType you want to version
