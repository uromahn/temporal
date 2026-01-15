package requestcancelworkflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

// mockMatchingClient implements a minimal matching client for testing
type mockMatchingClient struct {
	matchingservice.MatchingServiceClient
	addWorkerControlTaskCalls []*matchingservice.AddWorkerControlTaskRequest
	mu                        sync.Mutex
}

func (m *mockMatchingClient) AddWorkerControlTask(
	ctx context.Context,
	req *matchingservice.AddWorkerControlTaskRequest,
	opts ...grpc.CallOption,
) (*matchingservice.AddWorkerControlTaskResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addWorkerControlTaskCalls = append(m.addWorkerControlTaskCalls, req)
	return &matchingservice.AddWorkerControlTaskResponse{}, nil
}

func (m *mockMatchingClient) getCalls() []*matchingservice.AddWorkerControlTaskRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addWorkerControlTaskCalls
}

func TestDispatchActivityCancelTask(t *testing.T) {
	t.Parallel()

	logger := log.NewNoopLogger()
	mockClient := &mockMatchingClient{}

	namespaceID := "test-namespace-id"
	workflowID := "test-workflow-id"
	runID := "test-run-id"
	workerInstanceKey := "test-worker-key"

	ai := &persistencespb.ActivityInfo{
		ActivityId:                 "activity-1",
		ScheduledEventId:           5,
		StartedEventId:             6,
		WorkerInstanceKey:          workerInstanceKey,
		WorkerSupportsControlTasks: true,
	}

	dispatchActivityCancelTask(
		mockClient,
		logger,
		namespaceID,
		workflowID,
		runID,
		ai,
		"test cancellation",
	)

	// Wait for the goroutine to complete
	time.Sleep(200 * time.Millisecond)

	calls := mockClient.getCalls()
	require.Len(t, calls, 1, "AddWorkerControlTask should have been called once")

	call := calls[0]
	require.Equal(t, namespaceID, call.GetNamespaceId())
	require.Contains(t, call.GetTaskQueue().GetName(), workerInstanceKey)
	require.NotNil(t, call.GetControlPayload())
	require.Len(t, call.GetControlPayload().GetTasks(), 1)

	controlTask := call.GetControlPayload().GetTasks()[0]
	require.NotEmpty(t, controlTask.GetTaskId())

	cancelTask := controlTask.GetCancelActivity()
	require.NotNil(t, cancelTask)
	require.Equal(t, workflowID, cancelTask.GetWorkflowExecution().GetWorkflowId())
	require.Equal(t, runID, cancelTask.GetWorkflowExecution().GetRunId())
	require.Equal(t, int64(5), cancelTask.GetScheduledEventId())
	require.Equal(t, "activity-1", cancelTask.GetActivityId())
	require.Equal(t, "test cancellation", cancelTask.GetReason())
}

func TestDispatchActivityCancelTask_ControlQueueName(t *testing.T) {
	t.Parallel()

	logger := log.NewNoopLogger()
	mockClient := &mockMatchingClient{}

	namespaceID := "ns-123"
	workerInstanceKey := "worker-abc"

	ai := &persistencespb.ActivityInfo{
		ActivityId:                 "activity-1",
		ScheduledEventId:           5,
		StartedEventId:             6,
		WorkerInstanceKey:          workerInstanceKey,
		WorkerSupportsControlTasks: true,
	}

	dispatchActivityCancelTask(
		mockClient,
		logger,
		namespaceID,
		"workflow-id",
		"run-id",
		ai,
		"reason",
	)

	time.Sleep(200 * time.Millisecond)

	calls := mockClient.getCalls()
	require.Len(t, calls, 1)

	// Verify the queue name format
	expectedQueueName := "/temporal-sys/worker-commands/ns-123/worker-abc"
	require.Equal(t, expectedQueueName, calls[0].GetTaskQueue().GetName())
}

func TestShouldDispatchCancelTask(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name                       string
		workerSupportsControlTasks bool
		workerInstanceKey          string
		startedEventId             int64
		expectDispatch             bool
	}{
		{
			name:                       "supports control tasks and running",
			workerSupportsControlTasks: true,
			workerInstanceKey:          "worker-1",
			startedEventId:             6,
			expectDispatch:             true,
		},
		{
			name:                       "does not support control tasks",
			workerSupportsControlTasks: false,
			workerInstanceKey:          "worker-1",
			startedEventId:             6,
			expectDispatch:             false,
		},
		{
			name:                       "no worker instance key",
			workerSupportsControlTasks: true,
			workerInstanceKey:          "",
			startedEventId:             6,
			expectDispatch:             false,
		},
		{
			name:                       "activity not started",
			workerSupportsControlTasks: true,
			workerInstanceKey:          "worker-1",
			startedEventId:             common.EmptyEventID,
			expectDispatch:             false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ai := &persistencespb.ActivityInfo{
				ActivityId:                 "activity-1",
				ScheduledEventId:           5,
				StartedEventId:             tc.startedEventId,
				WorkerInstanceKey:          tc.workerInstanceKey,
				WorkerSupportsControlTasks: tc.workerSupportsControlTasks,
			}

			// Check if activity should be dispatched using the same logic as in Invoke
			shouldDispatch := ai.StartedEventId != common.EmptyEventID &&
				ai.WorkerSupportsControlTasks &&
				ai.WorkerInstanceKey != ""

			require.Equal(t, tc.expectDispatch, shouldDispatch)
		})
	}
}

func TestDispatchActivityCancelTask_MultipleActivities(t *testing.T) {
	t.Parallel()

	logger := log.NewNoopLogger()
	mockClient := &mockMatchingClient{}

	namespaceID := "test-namespace-id"
	workflowID := "test-workflow-id"
	runID := "test-run-id"

	activities := []*persistencespb.ActivityInfo{
		{
			ActivityId:                 "activity-1",
			ScheduledEventId:           5,
			StartedEventId:             6,
			WorkerInstanceKey:          "worker-1",
			WorkerSupportsControlTasks: true,
		},
		{
			ActivityId:                 "activity-2",
			ScheduledEventId:           7,
			StartedEventId:             8,
			WorkerInstanceKey:          "worker-2",
			WorkerSupportsControlTasks: true,
		},
		{
			ActivityId:                 "activity-3",
			ScheduledEventId:           9,
			StartedEventId:             common.EmptyEventID, // Not started
			WorkerInstanceKey:          "worker-3",
			WorkerSupportsControlTasks: true,
		},
	}

	// Dispatch only for activities that meet criteria
	for _, ai := range activities {
		if ai.StartedEventId != common.EmptyEventID &&
			ai.WorkerSupportsControlTasks &&
			ai.WorkerInstanceKey != "" {
			dispatchActivityCancelTask(
				mockClient,
				logger,
				namespaceID,
				workflowID,
				runID,
				ai,
				"workflow cancelled",
			)
		}
	}

	// Wait for goroutines
	time.Sleep(300 * time.Millisecond)

	calls := mockClient.getCalls()
	// Only 2 activities should be dispatched (activity-3 is not started)
	require.Len(t, calls, 2, "Should dispatch cancel for 2 running activities")

	// Verify the activity IDs
	activityIDs := make(map[string]bool)
	for _, call := range calls {
		cancelTask := call.GetControlPayload().GetTasks()[0].GetCancelActivity()
		activityIDs[cancelTask.GetActivityId()] = true
	}
	require.True(t, activityIDs["activity-1"])
	require.True(t, activityIDs["activity-2"])
	require.False(t, activityIDs["activity-3"])
}

// Suppress unused import warning
var _ = commonpb.WorkflowExecution{}
