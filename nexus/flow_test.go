package nexus

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestNexusFlow(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err, "Failed to start miniredis")
	defer mr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err, "Failed to connect to MongoDB")
	defer mongoClient.Disconnect(ctx)

	logger := zerolog.New(os.Stdout).With().Logger()
	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	args := NexusFlowArgs{
		Key:                 "test-event-queue",
		Client:              redisClient,
		Logger:              logger,
		LeaseTTL:            5 * time.Second,
		MaxQueueLength:      100,
		DbClient:            mongoClient,
		OperationTimeout:    10 * time.Second,
		MaxAttemptsPerEvent: 3,
		ScanAndFixInterval:  1 * time.Second,
	}

	flow, err := NewNexusFlow(ctx, args)
	require.NoError(t, err, "Failed to initialize NexusFlow")
	defer flow.Close(1 * time.Second)

	t.Run("ComponentState", func(t *testing.T) {
		state := flow.ComponentState(ctx)
		assert.Equal(t, "running", state.QueueState, "Redis queue should be running")
		assert.Equal(t, "running", state.DatabaseState, "MongoDB should be running")
	})

	t.Run("PushAndPullEvent", func(t *testing.T) {
		event := EventDetail{
			Id:          primitive.NewObjectID(),
			Payload:     `{"message": "Test event"}`,
			MaxAttempts: 3,
			Attempts:    0,
			State:       "pending",
		}

		err := flow.Push(ctx, event)
		require.NoError(t, err, "Failed to push event")

		dbEvent, err := flow.repo.FindOne(ctx, bson.D{{Key: "_id", Value: event.Id}})
		require.NoError(t, err, "Failed to fetch event from MongoDB")
		assert.Equal(t, event.Id, dbEvent.Id, "Event ID should match")
		assert.False(t, dbEvent.Dead, "Event should not be in dead-letter queue")
		assert.True(t, dbEvent.Queued, "Event should be marked as queued")

		pulledEvent, err := flow.Pull(ctx)
		require.NoError(t, err, "Failed to pull event")
		assert.Equal(t, dbEvent.Id, pulledEvent.Id, "Pulled event ID should match")
		assert.Equal(t, dbEvent.Payload, pulledEvent.Payload, "Pulled event payload should match")
	})

	t.Run("LeaseManagement", func(t *testing.T) {
		event := EventDetail{
			Id:          primitive.NewObjectID(),
			Payload:     `{"message": "Lease test"}`,
			MaxAttempts: 3,
			Attempts:    0,
			State:       "pending",
		}

		err := flow.Push(ctx, event)
		require.NoError(t, err, "Failed to push event")
		result, err := flow.dequeue(ctx)
		require.NoError(t, err, "Failed to dequeue event")

		var pulledEvent EventDetail
		err = json.Unmarshal([]byte(result.eventData), &pulledEvent)
		require.NoError(t, err, "Failed to deserialize event")

		isValid, err := flow.CheckLease(ctx, pulledEvent.Id.Hex(), result.leaseValue)
		require.NoError(t, err, "Failed to check lease")
		assert.True(t, isValid, "Lease should be valid")

		err = flow.ExtendLease(ctx, pulledEvent.Id.Hex(), result.leaseValue)
		require.NoError(t, err, "Failed to extend lease")

		err = flow.ReleaseLease(ctx, pulledEvent.Id.Hex())
		require.NoError(t, err, "Failed to release lease")

		isValid, err = flow.CheckLease(ctx, pulledEvent.Id.Hex(), result.leaseValue)
		assert.Error(t, err, "Expected error for non-existent lease")
		assert.False(t, isValid, "Lease should not be valid after release")
	})

	t.Run("DeadLetterQueue", func(t *testing.T) {
		event := EventDetail{
			Id:          primitive.NewObjectID(),
			Payload:     `{"message": "Dead letter test"}`,
			MaxAttempts: 2,
			Attempts:    2,
			State:       "pending",
		}

		err := flow.Push(ctx, event)
		require.NoError(t, err, "Failed to push event")

		dbEvent, err := flow.repo.FindOne(ctx, bson.D{{Key: "_id", Value: event.Id}})
		require.NoError(t, err, "Failed to fetch event from MongoDB")
		assert.True(t, dbEvent.Dead, "Event should be in dead-letter queue")
		assert.False(t, dbEvent.Queued, "Event should not be queued")

		_, err = flow.Pull(ctx)
		assert.Error(t, err, "Expected error when pulling from empty queue")
	})

	t.Run("ExpiredLeaseHandling", func(t *testing.T) {
		event := EventDetail{
			Id:          primitive.NewObjectID(),
			Payload:     `{"message": "Expired lease test"}`,
			MaxAttempts: 3,
			Attempts:    0,
			State:       "pending",
		}

		err := flow.Push(ctx, event)
		require.NoError(t, err, "Failed to push event")

		pulledEvent, err := flow.Pull(ctx)
		require.NoError(t, err, "Failed to dequeue event")

		mr.FastForward(10 * time.Second)

		_, err = flow.Pull(ctx)
		require.Error(t, err, "Failed to pull re-queued event")
		assert.Equal(t, event.Id, pulledEvent.Id, "Re-queued event ID should match")
	})

	t.Run("QueueFull", func(t *testing.T) {

		args.MaxQueueLength = 1
		flow, err := NewNexusFlow(ctx, args)
		require.NoError(t, err, "Failed to initialize NexusFlow")

		event1 := EventDetail{
			Id:          primitive.NewObjectID(),
			Payload:     `{"message": "Queue full test 1"}`,
			MaxAttempts: 3,
			Attempts:    0,
			State:       "pending",
		}
		err = flow.Push(ctx, event1)
		require.NoError(t, err, "Failed to push first event")

		event2 := EventDetail{
			Id:          primitive.NewObjectID(),
			Payload:     `{"message": "Queue full test 2"}`,
			MaxAttempts: 3,
			Attempts:    0,
			State:       "pending",
		}
		err = flow.Push(ctx, event2)
		assert.Error(t, err, "Expected error when pushing to full queue")
		assert.Contains(t, err.Error(), "queue is full", "Error should indicate full queue")
	})
}

func generateEvent() EventDetail {
	return EventDetail{
		Id:          primitive.NewObjectID(),
		Payload:     `{"message": "test event"}`,
		MaxAttempts: 3,
		Attempts:    0,
		State:       "pending",
	}
}

func setupNexusFlow(b *testing.B) (*NexusFlow, context.Context, func()) {
	mr, err := miniredis.Run()
	require.NoError(b, err, "Failed to start miniredis")

	ctx, cancel := context.WithCancel(context.Background())
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(b, err, "Failed to connect to MongoDB")

	logger := zerolog.New(os.Stdout).With().Logger()
	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	args := NexusFlowArgs{
		Key:                 "test-event-queue",
		Client:              redisClient,
		Logger:              logger,
		LeaseTTL:            5 * time.Second,
		MaxQueueLength:      100000,
		DbClient:            mongoClient,
		OperationTimeout:    5 * time.Second,
		MaxAttemptsPerEvent: 3,
		ScanAndFixInterval:  1 * time.Second,
	}

	flow, err := NewNexusFlow(ctx, args)
	if err != nil {
		require.NoErrorf(b, err, "Failed to initialize NexusFlow: %v", err)
	}

	cleanup := func() {
		flow.Close(5 * time.Second)
		err := mongoClient.Disconnect(ctx)
		require.NoError(b, err, "Failed to disconnect MongoDB")
		err = redisClient.Close()
		require.NoError(b, err, "Failed to close Redis client")
		mr.Close()
		cancel()
	}

	return flow, ctx, cleanup
}

func BenchmarkPush(b *testing.B) {
	flow, ctx, cleanup := setupNexusFlow(b)
	defer cleanup()

	for b.Loop() {
		if err := flow.Push(ctx, generateEvent()); err != nil {
			b.Fatalf("Push failed: %v", err)
		}
	}
}

func BenchmarkPull(b *testing.B) {
	flow, ctx, cleanup := setupNexusFlow(b)
	defer cleanup()

	for range 100 {
		event := generateEvent()
		if err := flow.Push(ctx, event); err != nil {
			b.Fatalf("Push failed during setup: %v", err)
		}
	}

	for b.Loop() {
		event, err := flow.Pull(ctx)
		if err != nil && err.Error() != "no event present in queue at the moment" {
			b.Fatalf("Pull failed: %v", err)
		}
		if event != nil {
			// Update the event to reflect re-queueing
			event.Queued = false // Reset Queued status to allow re-queueing
			if err := flow.Push(ctx, *event); err != nil {
				b.Fatalf("Re-queue failed: %v", err)
			}
		}
	}
}

func BenchmarkConcurrentPushPull(b *testing.B) {
	flow, ctx, cleanup := setupNexusFlow(b)
	defer cleanup()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := generateEvent()
			if err := flow.Push(ctx, event); err != nil {
				b.Fatalf("Push failed: %v", err)
			}
			if _, err := flow.Pull(ctx); err != nil && err.Error() != "no event present in queue at the moment" {
				b.Fatalf("Pull failed: %v", err)
			}
		}
	})
}
