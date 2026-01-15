package requestcancelworkflow

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	req *historyservice.RequestCancelWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
	logger log.Logger,
) (resp *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), req.CancelRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.CancelRequest
	parentExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.GetChildWorkflowOnly()
	workflowID := request.WorkflowExecution.WorkflowId
	runID := request.WorkflowExecution.RunId
	firstExecutionRunID := request.FirstExecutionRunId
	if len(firstExecutionRunID) != 0 {
		runID = ""
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				// the request to cancel this workflow is a success even
				// if the target workflow has already finished
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			// There is a workflow execution currently running with the WorkflowID.
			// If user passed in a FirstExecutionRunID with the request to allow cancel to work across runs then
			// let's compare the FirstExecutionRunID on the request to make sure we cancel the correct workflow
			// execution.
			executionInfo := mutableState.GetExecutionInfo()
			if len(firstExecutionRunID) > 0 && executionInfo.FirstExecutionRunId != firstExecutionRunID {
				return nil, consts.ErrWorkflowExecutionNotFound
			}

			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowId
				parentRunID := executionInfo.ParentRunId
				if parentExecution.GetWorkflowId() != parentWorkflowID ||
					parentExecution.GetRunId() != parentRunID {
					return nil, consts.ErrWorkflowParent
				}
			}

			isCancelRequested := mutableState.IsCancelRequested()
			if isCancelRequested {
				// since cancellation is idempotent
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			if _, err := mutableState.AddWorkflowExecutionCancelRequestedEvent(req); err != nil {
				return nil, err
			}

			// Dispatch cancel tasks to all running activities that support control tasks
			if matchingClient != nil {
				pendingActivities := mutableState.GetPendingActivityInfos()
				executionState := mutableState.GetExecutionState()
				for _, ai := range pendingActivities {
					if ai.StartedEventId != common.EmptyEventID &&
						ai.WorkerSupportsControlTasks &&
						ai.WorkerInstanceKey != "" {
						dispatchActivityCancelTask(
							matchingClient,
							logger,
							namespaceID.String(),
							executionInfo.WorkflowId,
							executionState.RunId,
							ai,
							request.GetReason(),
						)
					}
				}
			}

			return api.UpdateWorkflowWithNewWorkflowTask, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
}

// dispatchActivityCancelTask sends a cancel task to the worker's control queue.
// This is best-effort - failures are logged but don't fail the cancel request.
func dispatchActivityCancelTask(
	matchingClient matchingservice.MatchingServiceClient,
	logger log.Logger,
	namespaceID string,
	workflowID string,
	runID string,
	ai *persistencespb.ActivityInfo,
	reason string,
) {
	// Construct the worker control queue name
	controlQueueName := fmt.Sprintf("/temporal-sys/worker-commands/%s/%s", namespaceID, ai.WorkerInstanceKey)

	// Generate a unique task ID for deduplication
	taskID := fmt.Sprintf("%s-%s-%d-workflow-cancel", runID, ai.ActivityId, ai.ScheduledEventId)

	cancelTask := &workerpb.CancelActivityTask{
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		ScheduledEventId: ai.ScheduledEventId,
		ActivityId:       ai.ActivityId,
		Reason:           reason,
	}

	controlPayload := &workerpb.WorkerControlPayload{
		Tasks: []*workerpb.WorkerControlTask{
			{
				TaskId: taskID,
				Task: &workerpb.WorkerControlTask_CancelActivity{
					CancelActivity: cancelTask,
				},
			},
		},
	}

	// Send async to avoid blocking the cancel request
	go func() {
		dispatchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := matchingClient.AddWorkerControlTask(dispatchCtx, &matchingservice.AddWorkerControlTaskRequest{
			NamespaceId: namespaceID,
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: controlQueueName,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ControlPayload: controlPayload,
		})
		if err != nil {
			logger.Info("Failed to dispatch activity cancel task for workflow cancellation",
				tag.WorkflowNamespaceID(namespaceID),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.WorkflowScheduledEventID(ai.ScheduledEventId),
				tag.Error(err),
			)
		} else {
			logger.Debug("Dispatched activity cancel task for workflow cancellation",
				tag.WorkflowNamespaceID(namespaceID),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.WorkflowScheduledEventID(ai.ScheduledEventId),
			)
		}
	}()
}
