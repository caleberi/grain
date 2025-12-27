package nexus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// State represents the possible states of an event in its processing lifecycle.
type State string

// String returns the string representation of the State.
func (s State) String() string { return string(s) }

// State constants define the lifecycle stages of an event.
const (
	// Idle indicates no events are being processed.
	Submitted State = "SUMBITTED"
	// Pending indicates an event is received and awaiting processing.
	Pending State = "PENDING"
	// Processing indicates an event is being processed by a plugin.
	Processing State = "PROCESSING"
	// Completed indicates an event was successfully processed.
	Completed State = "COMPLETED"
	// Failed indicates an event failed after exhausting retries.
	Failed State = "FAILED"
	// Cancelled indicates an event was stopped before completion.
	Cancelled State = "CANCELLED"
	// Received indicates an event was consumed from the queue.
	Received State = "RECIEVED"
	// Retrial indicates an event is being retried after a failure.
	Retrial State = "RETRIAL"
	// Started indicates an event has begun processing.
	Started State = "STARTED"
)

// PluginTask defines a function signature for plugin execution.
// It takes a context and JSON-encoded arguments, returning a result and an error.
type PluginTask = func(ctx context.Context, argsJsonArg string) (any, error)

type EventDetail struct {
	Id             primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	State          string             `json:"state,omitempty" bson:"state,omitempty,omitzero"`
	Payload        string             `json:"payload,omitempty" bson:"payload,omitempty,omitzero"`
	Result         string             `json:"result,omitempty" bson:"result,omitempty,omitzero"`
	Attempts       int                `json:"attempts,omitempty" bson:"attempts,omitzero"`
	MaxAttempts    int                `json:"max_attempts,omitempty" bson:"max_attempts,omitzero"`
	CreatedAt      time.Time          `json:"created_at,omitzero" bson:"created_at,omitzero"`
	UpdatedAt      time.Time          `json:"updated_at,omitzero" bson:"updated_at,omitzero"`
	CompletedAt    *time.Time         `json:"completed_at,omitzero" bson:"completed_at,omitzero"`
	ErrorMessage   string             `json:"error,omitempty" bson:"error,omitempty,omitzero"`
	DelegationType string             `json:"delegation_type" bson:"delegation_type,omitzero"`
	Version        int                `json:"version" bson:"version,omitzero"`
	Stored         bool               `json:"stored" bson:"stored,omitzero"`
	Queued         bool               `json:"queued" bson:"queued,omitzero"`
	Dead           bool               `json:"dead" bson:"dead,omitzero"`
	Priority       uint8              `json:"priority,omitzero" bson:"priority,omitzero"`
	RetryCount     int                `json:"retry_count,omitzero" bson:"retry_count,omitzero"`
	RetryTimeout   int                `json:"retry_timeout,omitzero" bson:"retry_timeout,omitzero"`
	Eta            *time.Time         `json:"eta,omitzero" bson:"eta,omitzero"`
}

// EventDetailBuilder is a builder for EventDetail
type EventDetailBuilder struct {
	eventDetail *EventDetail
	err         error
}

// NewEventDetailBuilder creates a new EventDetailBuilder
func NewEventDetailBuilder() *EventDetailBuilder {
	now := time.Now()
	return &EventDetailBuilder{
		eventDetail: &EventDetail{
			Id:        primitive.NewObjectID(),
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
}

// WithState sets the State field
func (b *EventDetailBuilder) WithState(state string) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.State = state
	return b
}

// WithResult sets the Result field
func (b *EventDetailBuilder) WithResult(result string) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.Result = result
	return b
}

// WithAttempts sets the Attempts field
func (b *EventDetailBuilder) WithAttempts(attempts int) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	if attempts < 0 {
		b.err = fmt.Errorf("attempts cannot be negative")
		return b
	}
	b.eventDetail.Attempts = attempts
	return b
}

// WithMaxAttempts sets the MaxAttempts field
func (b *EventDetailBuilder) WithMaxAttempts(maxAttempts int) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	if maxAttempts < 0 {
		b.err = fmt.Errorf("max attempts cannot be negative")
		return b
	}
	b.eventDetail.MaxAttempts = maxAttempts
	return b
}

// WithCompletedAt sets the CompletedAt field
func (b *EventDetailBuilder) WithCompletedAt(completedAt *time.Time) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.CompletedAt = completedAt
	return b
}

// WithErrorMessage sets the ErrorMessage field
func (b *EventDetailBuilder) WithErrorMessage(errorMessage string) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.ErrorMessage = errorMessage
	return b
}

// WithDelegationType sets the DelegationType field
func (b *EventDetailBuilder) WithDelegationType(delegationType string) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.DelegationType = delegationType
	return b
}

// WithVersion sets the Version field
func (b *EventDetailBuilder) WithVersion(version int) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	if version < 0 {
		b.err = fmt.Errorf("version cannot be negative")
		return b
	}
	b.eventDetail.Version = version
	return b
}

// WithStored sets the Stored field
func (b *EventDetailBuilder) WithStored(stored bool) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.Stored = stored
	return b
}

// WithQueued sets the Queued field
func (b *EventDetailBuilder) WithQueued(queued bool) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.Queued = queued
	return b
}

// WithDead sets the Dead field
func (b *EventDetailBuilder) WithDead(dead bool) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.Dead = dead
	return b
}

// WithPriority sets the Priority field
func (b *EventDetailBuilder) WithPriority(priority uint8) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.Priority = priority
	return b
}

// WithRetryCount sets the RetryCount field
func (b *EventDetailBuilder) WithRetryCount(retryCount int) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	if retryCount < 0 {
		b.err = fmt.Errorf("retry count cannot be negative")
		return b
	}
	b.eventDetail.RetryCount = retryCount
	return b
}

// WithRetryTimeout sets the RetryTimeout field
func (b *EventDetailBuilder) WithRetryTimeout(retryTimeout int) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	if retryTimeout < 0 {
		b.err = fmt.Errorf("retry timeout cannot be negative")
		return b
	}
	b.eventDetail.RetryTimeout = retryTimeout
	return b
}

// WithEta sets the Eta field
func (b *EventDetailBuilder) WithEta(eta *time.Time) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	b.eventDetail.Eta = eta
	return b
}

// WithPayload sets the Payload field (called last due to potential error)
func (b *EventDetailBuilder) WithPayload(data map[string]any) *EventDetailBuilder {
	if b.err != nil {
		return b
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		b.err = fmt.Errorf("failed to marshal payload: %w", err)
		return b
	}
	b.eventDetail.Payload = string(bytes)
	return b
}

// Build returns the constructed EventDetail or any error that occurred
func (b *EventDetailBuilder) Build() (*EventDetail, error) {
	if b.err != nil {
		return nil, b.err
	}
	return b.eventDetail, nil
}

// SetPayload sets the Payload field directly
func (e *EventDetail) SetPayload(data map[string]any) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	e.Payload = string(bytes)
	return nil
}

// EventQueue defines the interface for managing a queue of events.
// It provides methods for pushing, pulling, and managing event leases, as well as checking queue connectivity.
type EventQueue interface {
	// Ping checks the connectivity of the queue.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// Returns:
	// - An error if the connectivity check fails.
	Ping(ctx context.Context) error

	// Push adds an event to the queue.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// - data: The event data to push, typically an EventDetail.
	// Returns:
	// - An error if serialization or pushing fails.
	Push(ctx context.Context, data EventDetail) error

	// Pull retrieves and removes an event from the queue, setting a lease for it.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// Returns:
	// - The retrieved event data and an error if the operation fails or the queue is empty.
	Pull(ctx context.Context) (EventDetail, error)

	// ExtendLease extends the lease for an event if the provided lease value matches.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// - eventId: The ID of the event whose lease is to be extended.
	// - currentLeaseValue: The current lease value to verify ownership.
	// Returns:
	// - An error if the lease extension fails or the lease is invalid/expired.
	ExtendLease(ctx context.Context, eventId string, currentLeaseValue string) error

	// CheckLease verifies if the lease for an event is still valid and matches the provided value.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// - eventId: The ID of the event to check.
	// - leaseValue: The lease value to verify.
	// Returns:
	// - A boolean indicating if the lease is valid and an error if the check fails.
	CheckLease(ctx context.Context, eventId string, leaseValue string) (bool, error)

	// ReleaseLease removes the lease for an event.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// - eventId: The ID of the event whose lease is to be released.
	// Returns:
	// - An error if the lease release fails.
	ReleaseLease(ctx context.Context, eventId string) error
}

// TaskStateQueue defines an interface for pushing and pulling task state updates to/from a queue.
type TaskStateQueue interface {
	// PushNewTaskState pushes a new task state to the queue.
	// Parameters:
	// - state: The task state to be pushed.
	// Returns:
	// - An error if the push operation fails.
	PushNewTaskState(state any) error

	// PullNewTaskState pulls the latest task state from the queue.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// Returns:
	// - The retrieved task state and an error if the operation fails or the queue is empty.
	PullNewTaskState(ctx context.Context) (any, error)

	// IsEmpty checks if the task state queue is empty.
	// Parameters:
	// - ctx: Context for cancellation and timeouts.
	// Returns:
	// - A boolean indicating whether the queue is empty.
	IsEmpty(ctx context.Context) bool
}

type TaskResult struct {
	Type  string `json:"type" bson:"type"`
	Value any    `json:"value" bson:"value"`
}

// TaskState represents the state of a task, including its identifier, status,
// result, completion status, and last update time.
type TaskState struct {
	// Id is a unique identifier for the task.
	Id string `json:"id"`
	// EventId is a unique identifier for the event associated with the task.
	EventId string `json:"_id"`
	// Status represents the current status of the task (e.g., Pending, Received, Started).
	Status string `json:"status"`
	Error  string `json:"error"`
	// Result contains the result or error message of the task.
	Result []TaskResult `json:"result"`
	// Completed indicates whether the task is completed.
	Completed bool `json:"completed"`
	// UpdatedAt records the timestamp of the last state update.
	UpdatedAt time.Time `json:"updatedAt"`
}
