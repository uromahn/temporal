// Package worker provides a CHASM component for tracking worker heartbeats and lifecycle.
// See README.md for more details.
package worker

import (
	"context"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/tasktoken"
)

const (
	Archetype chasm.Archetype = "Worker"

	// Default duration for worker leases if not specified in the request.
	defaultLeaseDuration = 1 * time.Minute
)

// Search attribute definitions for worker visibility.
var (
	WorkerStatusSearchAttribute = chasm.NewSearchAttributeKeyword("WorkerStatus", chasm.SearchAttributeFieldKeyword01)

	_ chasm.VisibilitySearchAttributesProvider = (*Worker)(nil)
)

// Worker is a Chasm component that tracks worker heartbeats and manages worker lifecycle.
type Worker struct {
	chasm.UnimplementedComponent

	// Persisted state.
	*workerstatepb.WorkerState
}

// NewWorker creates a new Worker component with ACTIVE status.
func NewWorker() *Worker {
	return &Worker{
		WorkerState: &workerstatepb.WorkerState{
			Status: workerstatepb.WORKER_STATUS_ACTIVE,
		},
	}
}

// LifecycleState returns the current lifecycle state of the worker.
func (w *Worker) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch w.Status {
	case workerstatepb.WORKER_STATUS_CLEANED_UP:
		return chasm.LifecycleStateCompleted
	default:
		// Active workers and inactive workers awaiting cleanup are still running.
		return chasm.LifecycleStateRunning
	}
}

// StateMachineState returns the current status.
func (w *Worker) StateMachineState() workerstatepb.WorkerStatus {
	return w.Status
}

// SetStateMachineState sets the status.
func (w *Worker) SetStateMachineState(status workerstatepb.WorkerStatus) {
	w.Status = status
}

// workerID returns the unique identifier for this worker.
func (w *Worker) workerID() string {
	if w.GetWorkerHeartbeat() == nil {
		return ""
	}
	return w.GetWorkerHeartbeat().GetWorkerInstanceKey()
}

// rescheduleActivities marks all activities bound to this worker as timed out
// and reschedules them for execution on other workers.
func (w *Worker) rescheduleActivities(
	ctx context.Context,
	logger log.Logger,
	historyClient historyservice.HistoryServiceClient,
) error {
	activityInfo := w.GetWorkerHeartbeat().GetActivityInfo()
	if activityInfo == nil {
		return nil
	}

	runningActivityTaskTokens := activityInfo.GetRunningActivityIds()
	if len(runningActivityTaskTokens) == 0 {
		return nil
	}

	logger.Info("Rescheduling activities for inactive worker",
		tag.WorkerID(w.workerID()),
		tag.NewInt("activity_count", len(runningActivityTaskTokens)))

	tokenSerializer := tasktoken.NewSerializer()
	var failedCount int
	var successCount int

	// Timeout each activity by calling history service
	timeoutFailure := &failurepb.Failure{
		Message: "Activity timed out due to worker becoming inactive",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
			},
		},
	}
	for _, taskToken := range runningActivityTaskTokens {
		// Deserialize the task token to get activity information
		token, err := tokenSerializer.Deserialize(taskToken)
		if err != nil {
			logger.Error("Failed to deserialize activity task token",
				tag.WorkerID(w.workerID()),
				tag.Error(err))
			failedCount++
			continue
		}

		// Call history service to fail the activity with timeout
		_, err = historyClient.RespondActivityTaskFailed(ctx, &historyservice.RespondActivityTaskFailedRequest{
			NamespaceId: token.NamespaceId,
			FailedRequest: &workflowservice.RespondActivityTaskFailedRequest{
				TaskToken: taskToken,
				Failure:   timeoutFailure,
				Identity:  "worker-liveness-monitor",
			},
		})

		// TODO: Handle retryable vs non-retryable errors
		if err != nil {
			logger.Error("Failed to timeout activity",
				tag.WorkerID(w.workerID()),
				tag.WorkflowID(token.WorkflowId),
				tag.WorkflowRunID(token.RunId),
				tag.WorkflowActivityID(token.ActivityId),
				tag.Error(err))
			failedCount++
			continue
		}

		logger.Info("Successfully timed out activity",
			tag.WorkerID(w.workerID()),
			tag.WorkflowID(token.WorkflowId),
			tag.WorkflowRunID(token.RunId),
			tag.WorkflowActivityID(token.ActivityId))
		successCount++
	}

	logger.Info("Completed activity rescheduling",
		tag.WorkerID(w.workerID()),
		tag.NewInt("success_count", successCount),
		tag.NewInt("failed_count", failedCount))

	// Return error if all activities failed to timeout
	if failedCount > 0 && successCount == 0 {
		return fmt.Errorf("failed to timeout all %d activities", failedCount)
	}

	return nil
}

// recordHeartbeat processes a heartbeat, updating worker state and extending the lease.
func (w *Worker) recordHeartbeat(ctx chasm.MutableContext, req *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
	// Extract worker heartbeat from request
	frontendReq := req.GetFrontendRequest()
	workerHeartbeat := frontendReq.GetWorkerHeartbeat()[0]

	// TODO: Honor the lease duration from the request.
	leaseDuration := defaultLeaseDuration

	w.WorkerHeartbeat = workerHeartbeat

	// Calculate lease deadline
	leaseDeadline := ctx.Now(w).Add(leaseDuration)

	// Apply appropriate state transition based on current status
	var err error
	switch w.Status {
	case workerstatepb.WORKER_STATUS_ACTIVE:
		err = TransitionActiveHeartbeat.Apply(ctx, w, EventHeartbeatReceived{
			LeaseDeadline: leaseDeadline,
		})
	case workerstatepb.WORKER_STATUS_INACTIVE:
		// Handle worker resurrection (example network partition, overloaded worker, etc.)
		err = TransitionResurrected.Apply(ctx, w, EventHeartbeatReceived{
			LeaseDeadline: leaseDeadline,
		})
	default:
		// CLEANED_UP or other states - not allowed
		err = fmt.Errorf("cannot record heartbeat for worker in state %v", w.Status)
	}

	if err != nil {
		return nil, err
	}

	return &workerstatepb.RecordHeartbeatResponse{}, nil
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface.
func (w *Worker) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		WorkerStatusSearchAttribute.Value(w.Status.String()),
	}
}
