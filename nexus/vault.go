// Package nexus provides functionality for managing task states with a custom backend
// and queue system for task state persistence and logging.
package nexus

import (
	"fmt"
	"time"

	"github.com/RichardKnop/machinery/v2/backends/iface"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/rs/zerolog"
)

const (
	// headerKey is the key used to store the task ID in the task signature headers.
	headerKey = "X-Task-ID"
	// eventIdKey is the key used to store the event ID in the task signature headers.
	eventIdKey = "X-Event-ID"
)

// NexusVault extends the Machinery backend with a custom queue and logging
// for task state management.
type NexusVault struct {
	iface.Backend                // Embedded Machinery backend interface for task state management.
	Queue         TaskStateQueue // Queue for storing task state updates.
	Log           zerolog.Logger // Logger for recording task state operations.
}

// SetStatePending marks a task as pending and pushes the state to the queue.
// Parameters:
// - signature: The task signature containing task metadata.
// Returns:
// - An error if the operation fails.
func (nv *NexusVault) SetStatePending(signature *tasks.Signature) error {
	if err := nv.Backend.SetStatePending(signature); err != nil {
		return err
	}
	return nv.pushNewTaskState(signature, Pending.String(), false, nil, "")
}

// SetStateReceived marks a task as received and pushes the state to the queue.
// Parameters:
// - signature: The task signature containing task metadata.
// Returns:
// - An error if the operation fails.
func (nv *NexusVault) SetStateReceived(signature *tasks.Signature) error {
	if err := nv.Backend.SetStateReceived(signature); err != nil {
		return err
	}
	return nv.pushNewTaskState(signature, Received.String(), false, nil, "")
}

// SetStateStarted marks a task as started and pushes the state to the queue.
// Parameters:
// - signature: The task signature containing task metadata.
// Returns:
// - An error if the operation fails.
func (nv *NexusVault) SetStateStarted(signature *tasks.Signature) error {
	if err := nv.Backend.SetStateStarted(signature); err != nil {
		return err
	}
	return nv.pushNewTaskState(signature, Started.String(), false, nil, "")
}

// SetStateRetry marks a task as retrying and pushes the state to the queue.
// Parameters:
// - signature: The task signature containing task metadata.
// Returns:
// - An error if the operation fails.
func (nv *NexusVault) SetStateRetry(signature *tasks.Signature) error {
	if err := nv.Backend.SetStateRetry(signature); err != nil {
		return err
	}
	return nv.pushNewTaskState(signature, Retrial.String(), false, nil, "")
}

// SetStateSuccess marks a task as successful and pushes the state to the queue.
// Parameters:
// - signature: The task signature containing task metadata.
// - results: The results of the task execution.
// Returns:
// - An error if the operation fails.
func (nv *NexusVault) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	if err := nv.Backend.SetStateSuccess(signature, results); err != nil {
		return err
	}
	return nv.pushNewTaskState(signature, Completed.String(), true, results, "")
}

// SetStateFailure marks a task as failed and pushes the state to the queue.
// Parameters:
// - signature: The task signature containing task metadata.
// - errStr: The error message describing the failure.
// Returns:
// - An error if the operation fails.
func (nv *NexusVault) SetStateFailure(signature *tasks.Signature, errStr string) error {
	if err := nv.Backend.SetStateFailure(signature, errStr); err != nil {
		return err
	}
	return nv.pushNewTaskState(signature, Failed.String(), true, nil, errStr)
}

// pushNewTaskState pushes a new task state to the queue with the specified status and metadata.
// Parameters:
// - signature: The task signature containing task metadata.
// - status: The status to set for the task (e.g., Pending, Received).
// - completed: Whether the task is completed.
// - results: The task execution results, if any.
// - errStr: The error message, if the task failed.
// Returns:
// - An error if the operation fails.
func (nv *NexusVault) pushNewTaskState(
	signature *tasks.Signature,
	status string,
	completed bool,
	results []*tasks.TaskResult,
	errStr string,
) error {
	// nv.Log.Info().Msgf("Processing task signature [%v]", signature)

	id, err := getFromHeader[string](signature, headerKey)
	if err != nil {
		nv.Log.Err(err).Msgf("Failed to extract task ID: %s", err.Error())
		return err
	}

	eventId, err := getFromHeader[string](signature, eventIdKey)
	if err != nil {
		nv.Log.Err(err).Msgf("Failed to extract event ID: %s", eventId)
		return err
	}

	var res []TaskResult
	for _, result := range results {
		res = append(res, TaskResult{
			Type:  result.Type,
			Value: result.Value,
		})
	}

	state := TaskState{
		Id:        id,
		EventId:   eventId,
		Status:    status,
		Result:    res,
		Error:     errStr,
		Completed: completed,
		UpdatedAt: time.Now(),
	}

	if err := nv.Queue.PushNewTaskState(state); err != nil {
		nv.Log.Err(err).Msgf("Failed to push task state: %v", state)
		return err
	}

	// if signature != nil {
	// 	if signature.RetryCount > 0 || signature.RetryTimeout > 0 || !signature.ETA.IsZero() {
	// 		return nil
	// 	}
	// }
	return nv.Backend.PurgeState(signature.UUID)
}

// getid extracts the task ID from the signature headers.
// Parameters:
// - signature: The task signature containing headers.
// Returns:
// - The task ID as a string and an error if the ID is missing or invalid.
func getFromHeader[T any](signature *tasks.Signature, key string) (T, error) {
	idHeader, isExist := signature.Headers[key]
	var ret T
	if !isExist {
		return ret, fmt.Errorf("'%s' header is missing in task signature", headerKey)
	}

	v, ok := idHeader.(T)
	if !ok {
		return ret, fmt.Errorf("'%s' header value '%v' is not a string", headerKey, idHeader)
	}

	return v, nil
}
