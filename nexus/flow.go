// Package nexus provides a robust event queueing system leveraging Redis for in-memory storage
// and MongoDB for persistent storage. It employs lease-based concurrency control to ensure
// unique event processing and includes a dead-letter queue for events that exceed their maximum
// retry attempts. The package is optimized for reliable, distributed event processing with
// atomic operations and comprehensive error handling.
//
// # Overview
//
// The core component, NexusFlow, orchestrates an event queue in Redis while persisting event
// details in MongoDB. It supports key operations such as pushing events to the queue, pulling
// events with lease-based concurrency control, extending or releasing leases, and managing
// expired leases. Events that surpass their retry limit are atomically moved to a dead-letter
// queue in MongoDB. The system is compatible with both standalone MongoDB instances and replica
// sets, utilizing transactions for atomicity in replica set environments.
//
// Key Components
//
//   - NexusFlow: The primary struct that manages interactions between the Redis queue and MongoDB repository.
//   - EventDetail: Defines the structure of an event, including fields like ID, payload, retry attempts, and state.
//   - NexusFlowArgs: Configuration struct for initializing a NexusFlow instance.
//   - NexusFlowComponentState: Provides the operational status of Redis and MongoDB components.
//
// # Dependencies
//
// The package requires the following dependencies:
//   - github.com/redis/go-redis/v9
//   - go.mongodb.org/mongo-driver/mongo
//   - quicdrop/internal/logging
//   - quicdrop/internal/mongod
//
// # Usage Example
//
// The following example demonstrates how to configure and use NexusFlow for event management.
// It assumes Redis and MongoDB servers are running locally on their default ports.
//
//	package main
//
//	import (
//		"context"
//		"fmt"
//		"time"
//
//		"github.com/redis/go-redis/v9"
//		"go.mongodb.org/mongo-driver/bson"
//		"go.mongodb.org/mongo-driver/mongo"
//		"go.mongodb.org/mongo-driver/mongo/options"
//		"quicdrop/internal/logging"
//		"quicdrop/nexus"
//	)
//
//	// EventDetail represents the structure of an event in the queue.
//	type EventDetail struct {
//		Id          string `bson:"_id"` // Unique identifier for the event.
//		Payload     string `bson:"payload"` // Event data in JSON format.
//		Attempts    int    `bson:"attempts"` // Number of processing attempts.
//		MaxAttempts int    `bson:"max_attempts"` // Maximum allowed retry attempts.
//		State       string `bson:"state"` // Current state of the event (e.g., "pending", "completed").
//		Stored      bool   `bson:"stored"` // Indicates if the event is persisted in MongoDB.
//		Dead        bool   `bson:"dead"` // Indicates if the event is in the dead-letter queue.
//		Queued      bool   `bson:"queued"` // Indicates if the event is in the Redis queue.
//	}
//
//	func main() {
//		// Initialize context for cancellation and timeouts
//		ctx := context.Background()
//
//		// Configure logger
//		logger := logging.NewConsoleLogger()
//
//		// Initialize Redis client
//		redisClient := redis.NewClient(&redis.Options{
//			Addr: "localhost:6379", // Replace with your Redis server address
//		})
//
//		// Initialize MongoDB client
//		mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
//		if err != nil {
//			logger.Errorf("Failed to connect to MongoDB: %v", err)
//			return
//		}
//		defer mongoClient.Disconnect(ctx)
//
//		// Configure NexusFlow parameters
//		args := nexus.NexusFlowArgs{
//			Key:                 "event-queue", // Redis key for the event queue
//			Client:              redisClient,
//			Logger:              logger,
//			LeaseTTL:            30 * time.Second, // Lease duration for events
//			MaxQueueLength:      1000, // Maximum events in the Redis queue
//			DbClient:            mongoClient,
//			OperationTimeout:    10 * time.Second, // Timeout for operations
//		}
//
//		// Initialize NexusFlow instance
//		flow, err := nexus.NewNexusFlow(ctx, args)
//		if err != nil {
//			logger.Errorf("Failed to initialize NexusFlow: %v", err)
//			return
//		}
//
//		// Check operational status of components
//		state := flow.ComponentState(ctx)
//		fmt.Printf("Queue State: %s, Database State: %s\n", state.QueueState, state.DatabaseState)
//
//		// Create and push an event to the queue
//		event := EventDetail{
//			Id:          "event-123",
//			Payload:     `{"message": "Hello, NexusFlow!"}`,
//			MaxAttempts: 3,
//			Attempts:    0,
//			State:       "pending",
//		}
//		if err := flow.Push(ctx, event); err != nil {
//			logger.Errorf("Failed to push event: %v", err)
//			return
//		}
//		logger.Infof("Pushed event: %v", event)
//
//		// Pull an event from the queue
//		pulledEvent, err := flow.Pull(ctx)
//		if err != nil {
//			logger.Errorf("Failed to pull event: %v", err)
//			return
//		}
//		if pulledEvent.Id != "" {
//			logger.Infof("Pulled event: %v", pulledEvent)
//
//			// Extend lease (in practice, leaseValue should be obtained from Pull)
//			leaseValue := "some-lease-value" // Placeholder for example
//			if err := flow.ExtendLease(ctx, pulledEvent.Id, leaseValue); err != nil {
//				logger.Errorf("Failed to extend lease: %v", err)
//			} else {
//				logger.Infof("Extended lease for event: %v", pulledEvent.Id)
//			}
//
//			// Verify lease validity
//			isValid, err := flow.CheckLease(ctx, pulledEvent.Id, leaseValue)
//			if err != nil {
//				logger.Errorf("Failed to check lease: %v", err)
//			} else {
//				logger.Infof("Lease valid for event %s: %v", pulledEvent.Id, isValid)
//			}
//
//			// Release lease after processing
//			if err := flow.ReleaseLease(ctx, pulledEvent.Id); err != nil {
//				logger.Errorf("Failed to release lease: %v", err)
//			} else {
//				logger.Infof("Released lease for event: %v", pulledEvent.Id)
//			}
//		}
//
//		// Retrieve event from MongoDB repository (Dead Event)
//		repo := flow.GetRepo()
//		eventFromDb, err := repo.FindOne(ctx, bson.D{{Key: "_id", Value: "event-123"}})
//		if err != nil {
//			logger.Errorf("Failed to fetch event from database: %v", err)
//		} else {
//			logger.Infof("Fetched event from database: %v", eventFromDb)
//		}
//	}
//
// # Setup Instructions
//
// To use the nexus package:
// 1. Ensure Redis and MongoDB servers are running and accessible.
// 2. Install dependencies using `go get` for the required packages.
// 3. Configure NexusFlow with appropriate Redis and MongoDB connection details.
// 4. Utilize NexusFlow methods to manage events, as demonstrated in the example.
//
// # Additional Notes
//
// - The package supports both standalone MongoDB and replica sets, using transactions for atomicity in replica set configurations.
// - Events exceeding their maximum retry attempts are automatically moved to the dead-letter queue.
// - The Pull method employs Lua scripts in Redis for atomic event retrieval and lease management, requiring cjson for JSON parsing.
// - Periodically run ScanAndDeleteExpiredLeases to clean up expired leases and re-queue eligible events.
// - Ensure Redis is configured with cjson support for Lua script JSON parsing.
package nexus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"grain/mongod"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// NexusFlowComponentState holds the operational status of NexusFlow components.
type NexusFlowComponentState struct {
	QueueState    string // Indicates the status of the Redis event queue (e.g., "running", "down").
	DatabaseState string // Indicates the status of the MongoDB database (e.g., "running", "down").
}

// NexusFlow manages an event queue using Redis as the primary store and MongoDB for persistence.
// It implements lease-based concurrency control to ensure events are processed uniquely and
// supports a dead-letter queue for events exceeding retry limits.
type NexusFlow struct {
	ctx                context.Context                 // Context for managing cancellation and timeouts.
	cancelFunc         context.CancelFunc              // Function to cancel the context.
	eventQueueKey      string                          // Redis key used for the event queue.
	client             *redis.Client                   // Redis client for queue operations.
	logger             zerolog.Logger                  // Logger for tracking operations and errors.
	leaseTTL           time.Duration                   // Duration for which an event lease is valid.
	repo               *mongod.Repository[EventDetail] // MongoDB repository for storing event details.
	maxQueueLength     int                             // Maximum number of events allowed in the Redis queue.
	operationTimeout   time.Duration                   // Timeout duration for database and Redis operations.
	clientInfo         mongoClientInfo                 // MongoDB client configuration (replica set or standalone).
	resolutionInterval time.Duration                   // Interval for scanning and fixing expired leases.
	errCh              chan error
	mu                 sync.RWMutex
}

// NexusFlowArgs holds configuration parameters for initializing a NexusFlow instance.
type NexusFlowArgs struct {
	Key                 string         // Redis key for the event queue.
	Client              *redis.Client  // Redis client for queue operations.
	Logger              zerolog.Logger // Logger for recording operations and errors.
	LeaseTTL            time.Duration  // Duration for lease validity; defaults to 30 seconds if zero.
	DbClient            *mongo.Client  // MongoDB client for database operations.
	OperationTimeout    time.Duration  // Timeout duration for operations.
	ScanAndFixInterval  time.Duration  // Interval for scanning and fixing expired leases.
	MaxAttemptsPerEvent int            // Maximum retry attempts per event.
	MaxQueueLength      int            // Maximum number of events in the Redis queue.
	KeyTTL              time.Duration
}

// mongoClientInfo stores configuration details about the MongoDB client.
type mongoClientInfo struct {
	IsReplicaSet bool // Indicates whether the client is connected to a replica set or sharded cluster.
}

type adminReplicaCmdInfo struct {
	Ok int `bson:"ok"`
}

func checkAndDefaultNexusFlowArgs(args *NexusFlowArgs) {
	if args.MaxQueueLength <= 0 {
		args.MaxQueueLength = 10000
	}
	if args.LeaseTTL <= 0 {
		args.LeaseTTL = 30 * time.Second
	}
	if args.ScanAndFixInterval <= 0 {
		args.ScanAndFixInterval = 2 * time.Minute
	}
	if args.OperationTimeout <= 0 {
		args.OperationTimeout = 10 * time.Second
	}
}

func ensureQueueKeyExpiry(ctx context.Context, client *redis.Client, queueKey string, expiry time.Duration) bool {
	exists, err := client.Exists(ctx, queueKey).Result()
	if err != nil {
		return false
	}
	if exists == 0 {
		return false // Key does not exist
	}
	ok, err := client.Expire(ctx, queueKey, expiry).Result()
	if err != nil {
		return false
	}
	return ok
}

// NewNexusFlow initializes a new NexusFlow instance with Redis and MongoDB clients.
// It sets up the event queue and MongoDB repository, creating a unique index on event IDs.
// A background goroutine is started to periodically scan and fix expired leases.
// Parameters:
//   - ctx: Context for managing cancellation and timeouts.
//   - args: Configuration parameters for NexusFlow.
//
// Returns:
//   - A pointer to the initialized NexusFlow instance or an error if setup fails.
func NewNexusFlow(ctx context.Context, args NexusFlowArgs) (*NexusFlow, error) {
	var serverInfo adminReplicaCmdInfo
	var clientInfo mongoClientInfo

	checkAndDefaultNexusFlowArgs(&args)
	database := args.DbClient.Database("nexus-flow-events", options.Database())
	adminDatabase := args.DbClient.Database("admin", options.Database())
	collection := database.Collection("events", options.Collection())
	repo := mongod.NewRepositoryWithOptions(
		mongod.WithClient[EventDetail](args.DbClient),
		mongod.WithCollection[EventDetail](collection),
	)

	// https://www.mongodb.com/docs/manual/reference/command/replSetGetStatus
	dctx, cancelFunc := context.WithTimeout(ctx, args.OperationTimeout)
	defer cancelFunc()
	commandResult := adminDatabase.RunCommand(dctx, bson.D{{Key: "replSetGetStatus", Value: 1}})
	if err := commandResult.Decode(&serverInfo); err != nil {
		if !strings.Contains(err.Error(), "not running with --replSet") {
			return nil, fmt.Errorf("failed to get MongoDB client info: %w", err)
		}
	}

	if serverInfo.Ok == 1 {
		clientInfo = mongoClientInfo{IsReplicaSet: true}
	}

	ctx, cancelFunc = context.WithCancel(ctx)
	nexusFlow := &NexusFlow{
		repo:               repo,
		ctx:                ctx,
		clientInfo:         clientInfo,
		cancelFunc:         cancelFunc,
		client:             args.Client,
		eventQueueKey:      args.Key,
		logger:             args.Logger,
		leaseTTL:           args.LeaseTTL,
		maxQueueLength:     args.MaxQueueLength,
		operationTimeout:   args.OperationTimeout,
		resolutionInterval: args.ScanAndFixInterval,
		errCh:              make(chan error),
	}

	rctx, cancelFunc := context.WithTimeout(ctx, args.OperationTimeout)
	defer cancelFunc()
	ensureQueueKeyExpiry(rctx, nexusFlow.client, nexusFlow.eventQueueKey, args.KeyTTL)

	go func(flow *NexusFlow) {
		ticker := time.NewTicker(flow.resolutionInterval)
		defer ticker.Stop()

		var periodicQueueRunner = func(flow *NexusFlow) {
			ctx, cancelFunc := context.WithTimeout(nexusFlow.ctx, nexusFlow.operationTimeout)
			defer cancelFunc()
			filter := bson.D{
				{
					Key: "$or", Value: bson.A{
						bson.D{{Key: "dead", Value: bson.D{{Key: "$ne", Value: true}}}},
						bson.D{{Key: "state", Value: bson.D{{Key: "$ne", Value: Completed}}}},
					},
				},
				{Key: "delegation_type", Value: bson.D{{Key: "$ne", Value: ""}}},
				{
					Key: "$expr", Value: bson.D{
						{Key: "$lte", Value: bson.A{"$attempts", "$max_attempts"}},
					},
				},
			}
			if events, err := nexusFlow.repo.FindMany(
				ctx, filter, options.Find().SetBatchSize(50)); err != nil {
				nexusFlow.errCh <- err
			} else {
				if len(events) > 0 {
					flow.logger.Info().Msgf("Found %d events to re-queue", len(events))
					for i := range events {
						if events[i].Attempts >= events[i].MaxAttempts {
							events[i].Dead = true
							events[i].Queued = false
						}
					}
					ctx, cancelFunc := context.WithCancel(nexusFlow.ctx)
					defer cancelFunc()
					if err := flow.queueBatch(ctx, events); err != nil {
						nexusFlow.errCh <- err
					}
				}
			}
		}

		for {
			select {
			case <-flow.ctx.Done():
				return
			case <-ticker.C:
				periodicQueueRunner(flow)
			}
		}
	}(nexusFlow)
	return nexusFlow, nil
}

// queue adds an event to the Redis queue using a Lua script to ensure queue length limits are respected.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//   - event: The EventDetail to queue.
//
// Returns:
//   - An error if serialization or queue operations fail.
func (flow *NexusFlow) queue(ctx context.Context, event EventDetail) error {
	serialized, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to serialize event (event=%v): %w", event, err)
	}

	boundedPushScript := `
		-- Check the length of the queue
		local queueLength = redis.call("LLEN", KEYS[1])
		if queueLength >= tonumber(ARGV[1]) then
			-- If the queue is full, return an error
			return 0
		end;
		-- Push the event to the queue
		redis.call("RPUSH", KEYS[1], ARGV[2]);
		return 1
	`
	result, err := flow.client.Eval(ctx, boundedPushScript,
		[]string{flow.eventQueueKey}, flow.maxQueueLength, serialized).Result()
	if err != nil {
		if strings.Contains(err.Error(), "redis: nil") {
			return redis.Nil
		}
		return fmt.Errorf("failed to execute Lua script for pushing event (event=%v): %w", event, err)
	}

	// Verify Lua script result
	queued, ok := result.(int64)
	if !ok {
		return fmt.Errorf("invalid result type from Lua script (event=%v): got %T", event, result)
	}
	if queued == 0 {
		return fmt.Errorf("queue is full, cannot push event (event=%v)", event)
	}
	return nil
}

func (flow *NexusFlow) queueBatch(ctx context.Context, events []EventDetail) error {
	if len(events) == 0 {
		return nil
	}

	serializedEvents := make([]any, len(events))
	for i, event := range events {
		serialized, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to serialize event %d (event=%v): %w", i, event, err)
		}
		serializedEvents[i] = serialized
	}

	boundedPushScript := `
        -- Check the length of the queue
        local queueLength = redis.call("LLEN", KEYS[1])
        local maxLength = tonumber(ARGV[1])
        local batchSize = #ARGV - 1
        if queueLength + batchSize > maxLength then
            -- If the queue would overflow, return 0
            return 0
        end
        -- Push all events to the queue
        for i = 2, #ARGV do
            redis.call("RPUSH", KEYS[1], ARGV[i])
        end
        return 1
    `

	args := make([]any, 1+len(serializedEvents))
	args[0] = flow.maxQueueLength
	copy(args[1:], serializedEvents)

	result, err := flow.client.Eval(ctx, boundedPushScript, []string{flow.eventQueueKey}, args...).Result()
	if err != nil {
		if strings.Contains(err.Error(), "redis: nil") {
			return redis.Nil
		}
		return fmt.Errorf("failed to execute Lua script for batch pushing %d events: %w", len(events), err)
	}

	queued, ok := result.(int64)
	if !ok {
		return fmt.Errorf("invalid result type from Lua script for batch push: got %T", result)
	}
	if queued == 0 {
		return fmt.Errorf("queue capacity exceeded, cannot push %d events (current length: %d, max: %d)",
			len(events), flow.client.LLen(ctx, flow.eventQueueKey).Val(), flow.maxQueueLength)
	}
	return nil
}

// Push adds an event to the Redis queue and MongoDB repository.
// If the event exceeds its maximum retry attempts, it is moved to the dead-letter queue.
// For replica sets, operations are performed atomically within a transaction.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//   - event: The EventDetail to push.
//
// Returns:
//   - An error if validation, serialization, or database/queue operations fail.
func (flow *NexusFlow) Push(ctx context.Context, event EventDetail) error {
	if event.MaxAttempts < 0 {
		return fmt.Errorf("max attempts cannot be negative (attempts: %d)", event.MaxAttempts)
	}

	if event.Attempts >= event.MaxAttempts {
		event.Dead = true
	}

	if event.Id.IsZero() {
		event.Id = primitive.NewObjectID()
	}

	flow.mu.RLock()
	isReplicaSet := flow.clientInfo.IsReplicaSet
	flow.mu.RUnlock()

	if !isReplicaSet {
		// Check if the event exists in MongoDB
		filter := bson.D{{Key: "_id", Value: event.Id}}
		err := flow.checkEventInDatabase(ctx, filter)
		event.Stored = true
		// If the event doesn't exist, create it
		if errors.Is(err, mongo.ErrNoDocuments) {
			ctx, cancel := context.WithTimeout(flow.ctx, flow.operationTimeout)
			defer cancel()
			err = flow.logEventInDatabase(ctx, event)
			if err != nil {
				return fmt.Errorf("failed to log event in database: %w", err)
			}
		} else {
			// If the event exists, update it
			ctx, cancel := context.WithTimeout(flow.ctx, flow.operationTimeout)
			defer cancel()
			err = flow.updateEventLogInDatabase(ctx, event)
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				return fmt.Errorf("failed to update event in database: %w", err)
			}
		}

		// Queue the event in Redis if not dead
		if !event.Dead {
			ctx, cancel := context.WithTimeout(flow.ctx, flow.operationTimeout)
			defer cancel()
			if err := flow.queue(ctx, event); err != nil {
				return fmt.Errorf("failed to queue event: %w", err)
			}
			event.Queued = true
			// Update the event in MongoDB to reflect Queued status
			ctx, cancel = context.WithTimeout(flow.ctx, flow.operationTimeout)
			defer cancel()
			err = flow.updateEventLogInDatabase(ctx, event)
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				return fmt.Errorf("failed to update event in database: %w", err)
			}
		}
		return nil
	}

	// Replica set case
	if err := flow.checkReplicaSetStatus(ctx); err != nil {
		return fmt.Errorf("replica set not ready for transaction (event=%v): %w", event, err)
	}

	return flow.repo.WrapWithTransaction(
		ctx, event, func(
			ctx mongo.SessionContext,
			repo *mongod.Repository[EventDetail], data EventDetail) error {
			filter := bson.D{{Key: "_id", Value: data.Id}}
			err := flow.checkEventInDatabase(ctx, filter)
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				return fmt.Errorf("failed to check existing event: %w", err)
			}
			data.Stored = true
			if errors.Is(err, mongo.ErrNoDocuments) {
				err = flow.logEventInDatabase(ctx, data)
				if err != nil {
					return fmt.Errorf("failed to log event in database: %w", err)
				}
			} else {
				err = flow.updateEventLogInDatabase(ctx, data)
				if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
					return fmt.Errorf("failed to update event in database: %w", err)
				}
			}

			if !data.Dead {
				if err := flow.queue(ctx, data); err != nil {
					return fmt.Errorf("failed to queue event: %w", err)
				}
				data.Queued = true
				err = flow.updateEventLogInDatabase(ctx, data)
				if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
					return fmt.Errorf("failed to update event in database: %w", err)
				}
			}
			return nil
		},
		options.Session().SetDefaultWriteConcern(writeconcern.Majority()))
}

func (flow *NexusFlow) checkEventInDatabase(ctx context.Context, filter bson.D) error {
	flow.mu.RLock()
	defer flow.mu.RUnlock()
	_, err := flow.repo.FindOne(ctx, filter)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("failed to check existing event: %w", err)
	}
	return err
}

func (flow *NexusFlow) logEventInDatabase(ctx context.Context, event EventDetail) error {
	flow.mu.Lock()
	defer flow.mu.Unlock()
	_, err := flow.repo.Create(ctx, event)
	if err != nil {
		return err
	}
	return nil
}

func (flow *NexusFlow) updateEventLogInDatabase(ctx context.Context, event EventDetail) error {
	currentVersion := event.Version
	event.Version += 1

	filter := bson.D{
		{Key: "_id", Value: event.Id},
		{Key: "version", Value: currentVersion},
	}

	err := flow.checkEventInDatabase(ctx, filter)
	if err != nil || errors.Is(err, mongo.ErrNoDocuments) {
		return fmt.Errorf("failed to check existing event: %w", err)
	}

	flow.mu.Lock()
	defer flow.mu.Unlock()
	return flow.repo.UpdateOneById(
		ctx, event.Id, event,
		options.FindOneAndUpdate().
			SetUpsert(true).SetReturnDocument(options.After))
}

// Pull retrieves and removes an event from the Redis queue, setting a lease to prevent concurrent processing.
// It uses a Lua script for atomic event retrieval and lease setting, requiring cjson for JSON parsing in Redis.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//
// Returns:
//   - The deserialized EventDetail and an error if the queue is empty or operations fail.
func (flow *NexusFlow) Pull(ctx context.Context) (*EventDetail, error) {
	result, err := flow.dequeue(ctx)
	if err != nil {
		return nil, err
	}

	if result == nil {
		return nil, fmt.Errorf("no event present in queue at the moment")
	}

	var event EventDetail
	if err := json.Unmarshal([]byte(result.eventData), &event); err != nil {
		return nil, fmt.Errorf("failed to deserialize event data (event=%v) (err=%v)", event, err)
	}

	currentLeaseValue, err := flow.client.Get(ctx, result.leaseKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("lease expired or not found (event=%v)", event)
		}
		return nil, fmt.Errorf("failed to get lease value (event=%v) (err=%v)", event, err)
	}
	if currentLeaseValue != result.leaseValue {
		if err := flow.Push(ctx, event); err != nil {
			return nil, fmt.Errorf("failed to re-queue event after lease verification failure (event=%v) (err=%v)", event, err)
		}
		return nil, fmt.Errorf("lease verification failed (event=%v) (err=%v)", event, err)
	}
	return &event, nil
}

// dequeueResult holds the result of a dequeue operation, including lease key, lease value, and event data.
type dequeueResult struct {
	leaseKey   string // Redis key for the event lease.
	leaseValue string // Unique lease value for the event.
	eventData  string // Serialized event data.
}

// dequeue performs an atomic dequeue operation using a Lua script to pop an event and set its lease.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//
// Returns:
//   - A dequeueResult containing the lease key, lease value, and event data, or an error if the operation fails.
func (flow *NexusFlow) dequeue(ctx context.Context) (*dequeueResult, error) {
	luaScript := `
		-- Pop an event from the queue
		local event = redis.call("LPOP", KEYS[1])
		if not event then
			return {nil, nil}
		end

		-- Validate JSON decoding
		local status, event_data = pcall(cjson.decode, event)
		if not status then
			-- Log error and return nil if JSON is invalid
			redis.call("RPUSH", KEYS[1], event) -- Re-queue invalid event
			return {nil, nil}
		end

		-- Check for event_id
		local event_id = event_data.id
		if not event_id then
			-- Log error and return nil if event_id is missing
			-- redis.call("RPUSH", KEYS[1], event) -- Re-queue invalid event
			return {nil, nil}
		end

		-- Generate lease key (event:lease:<event_id>)
		local lease_key = "event:lease:" .. event_id

		-- Set lease with TTL if it doesn't exist
		local lease_value = ARGV[1]
		local ttl = ARGV[2]
		local lease_set = redis.call("SETNX", lease_key, lease_value)
		if lease_set == 1 then
			redis.call("EXPIRE", lease_key, ttl)
			return {event, lease_key}
		else
			-- Re-queue the event if lease acquisition fails
			redis.call("RPUSH", KEYS[1], event)
			return {nil, nil}
		end
	`
	leaseValue := uuid.New().String()
	result, err := flow.client.Eval(
		ctx, luaScript, []string{flow.eventQueueKey},
		leaseValue, int(flow.leaseTTL.Seconds())).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to execute Lua script for pull: %w", err)
	}

	resultSlice, ok := result.([]any)
	if !ok || len(resultSlice) != 2 || resultSlice[0] == nil {
		return nil, nil
	}

	eventData, ok := resultSlice[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid event data type from Lua script: got %T", resultSlice[0])
	}

	if eventData == "" {
		return nil, fmt.Errorf("missing event_id in event data")
	}

	// Validate JSON deserialization
	var event EventDetail
	if err := json.Unmarshal([]byte(eventData), &event); err != nil {
		return nil, fmt.Errorf("failed to deserialize event data: %w", err)
	}

	// Ensure event_id is present
	if event.Id.IsZero() {
		return nil, fmt.Errorf("missing event_id in event data")
	}

	leaseKey, ok := resultSlice[1].(string)
	if !ok {
		return nil, fmt.Errorf("invalid lease key type from Lua script: got %T", resultSlice[1])
	}

	ret := dequeueResult{
		leaseKey:   leaseKey,
		leaseValue: leaseValue,
		eventData:  eventData,
	}
	return &ret, nil
}

// ExtendLease extends the lease for an event if the provided lease value matches the current lease.
// It uses a Lua script to ensure atomicity during lease extension.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//   - eventId: The ID of the event whose lease is to be extended.
//   - currentLeaseValue: The current lease value to verify ownership.
//
// Returns:
//   - An error if the lease extension fails or the lease is invalid/expired.
func (flow *NexusFlow) ExtendLease(ctx context.Context, eventId string, currentLeaseValue string) error {
	leaseKey := fmt.Sprintf("event:lease:%s", eventId)
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("EXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
	result, err := flow.client.Eval(
		ctx, luaScript,
		[]string{leaseKey}, currentLeaseValue, int(flow.leaseTTL.Seconds())).Int()
	if err != nil {
		return fmt.Errorf("failed to extend lease (err=%v)", err)
	}
	if result == 0 {
		return fmt.Errorf("cannot extend lease for event %s: lease expired or invalid", eventId)
	}
	return nil
}

// Ping verifies connectivity to the Redis server.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//
// Returns:
//   - An error if the connectivity check fails.
func (flow *NexusFlow) Ping(ctx context.Context) error {
	if err := flow.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to ping Redis server (err=%v)", err)
	}
	flow.logger.Info().Msg("successfully pinged Redis server")
	return nil
}

// CheckLease verifies whether the lease for an event is valid and matches the provided lease value.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//   - eventId: The ID of the event to check.
//   - leaseValue: The lease value to verify.
//
// Returns:
//   - A boolean indicating if the lease is valid and an error if the check fails.
func (flow *NexusFlow) CheckLease(ctx context.Context, eventId string, leaseValue string) (bool, error) {
	leaseKey := fmt.Sprintf("event:lease:%s", eventId)
	currentValue, err := flow.client.Get(ctx, leaseKey).Result()
	if err != nil {
		if err == redis.Nil {
			return false, fmt.Errorf("no lease found with key %s", leaseKey)
		}
		return false, err
	}
	if currentValue == "" {
		return false, fmt.Errorf("no lease found with key %s", leaseKey)
	}
	return currentValue == leaseValue, nil
}

// ReleaseLease removes the lease for an event from Redis, allowing it to be reprocessed.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//   - eventId: The ID of the event whose lease is to be released.
//
// Returns:
//   - An error if the lease release fails.
func (flow *NexusFlow) ReleaseLease(ctx context.Context, eventId string) error {
	leaseKey := fmt.Sprintf("event:lease:%s", eventId)
	if err := flow.client.Del(ctx, leaseKey).Err(); err != nil {
		return fmt.Errorf("failed to release lease %w", err)
	}
	flow.logger.Info().Msgf("successfully released lease (event=%v)", eventId)
	return nil
}

// resolvePotentiallyNotDeadEvent scans for expired leases, deletes them, and re-queues eligible events.
// It runs periodically to ensure events with expired leases are reprocessed if not completed or failed.
// Returns:
//   - An error if the scan or re-queue operations fail.
func (flow *NexusFlow) ResolvePotentiallyNotDeadEvent() {
	ctx, cancel := context.WithTimeout(flow.ctx, flow.operationTimeout)
	defer cancel()
	iter := flow.client.Scan(ctx, 0, "event:lease:*", 100).Iterator()
	for iter.Next(ctx) {
		leaseKey := iter.Val()
		eventId := strings.TrimPrefix(leaseKey, "event:lease:")
		ttl, err := flow.client.TTL(ctx, leaseKey).Result()
		if err != nil {
			flow.errCh <- fmt.Errorf("failed to check TTL for lease :%v", err)
			continue
		}
		if ttl > 0 {
			continue
		}
		if err := flow.ReleaseLease(ctx, eventId); err != nil {
			flow.errCh <- fmt.Errorf("failed to delete expired lease : %s", err)
			continue
		}

		id, err := primitive.ObjectIDFromHex(eventId)
		if err != nil {
			flow.errCh <- fmt.Errorf("invalid mongodb id provided")
			continue
		}
		event, err := flow.repo.FindOneById(ctx, id)
		if err != nil {
			flow.errCh <- fmt.Errorf("failed to fetch event from database: %w , eventId = %s", err, id.Hex())
			continue
		}
		if !event.Dead || event.State != Completed.String() || event.State != Failed.String() || ttl <= 0 {
			if err := flow.Push(ctx, *event); err != nil {
				flow.errCh <- fmt.Errorf("failed to re-queue event (event=%v): %v", *event, err)
				continue
			}
		}
	}

	if err := iter.Err(); err != nil {
		flow.errCh <- fmt.Errorf("failed to scan lease keys: %w", err)
	}
}

// ComponentState checks the operational status of the Redis queue and MongoDB database.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//
// Returns:
//   - A NexusFlowComponentState indicating the operational status of the queue and database.
func (flow *NexusFlow) ComponentState(ctx context.Context) NexusFlowComponentState {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	queueState := "running"
	if err := flow.client.Ping(ctx).Err(); err != nil {
		queueState = "down"
		flow.logger.Err(err).Msg("failed to ping Redis server")
	}

	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	eventDbState := "running"
	if err := flow.repo.GetClient().Ping(ctx, readpref.Nearest()); err != nil {
		eventDbState = "down"
		flow.logger.Err(err).Msg("failed to ping MongoDB database")
	}

	return NexusFlowComponentState{
		QueueState:    queueState,
		DatabaseState: eventDbState,
	}
}

// checkReplicaSetStatus verifies the health and status of the MongoDB replica set.
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//
// Returns:
//   - An error if the replica set is not healthy or lacks a primary/secondary.
func (flow *NexusFlow) checkReplicaSetStatus(ctx context.Context) error {
	var result struct {
		Members []struct {
			Name     string `bson:"name"`
			Health   int    `bson:"health"`
			StateStr string `bson:"stateStr"`
		} `bson:"members"`
		Ok int `bson:"ok"`
	}

	admin := flow.repo.GetClient().Database("admin")
	ctx, cancel := context.WithTimeout(ctx, flow.operationTimeout)
	defer cancel()
	err := admin.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return fmt.Errorf("failed to get replica set status: %w", err)
	}
	if result.Ok != 1 {
		return fmt.Errorf("replica set status command failed: ok status: %d", result.Ok)
	}

	var primaryCount, secondaryCount int
	for _, member := range result.Members {
		if member.Health != 1 {
			flow.logger.Warn().Msgf("unhealthy member: %s", member.Name)
			continue
		}
		switch member.StateStr {
		case "PRIMARY":
			primaryCount++
		case "SECONDARY":
			secondaryCount++
		}
	}

	if primaryCount != 1 {
		return fmt.Errorf("invalid primary count: expected 1 primary, found %d", primaryCount)
	}
	if secondaryCount < 1 {
		return fmt.Errorf("insufficient secondary servers: found %d secondaries, need at least 1", secondaryCount)
	}

	flow.logger.Info().Msgf("replica set status check passed: %d primaries, %d secondaries", primaryCount, secondaryCount)
	return nil
}

// // GetRepo returns the MongoDB repository used for storing event details.
// // Returns:
// //   - A pointer to the MongoDB repository for EventDetail.
// func (flow *NexusFlow) GetRepo() *mongod.Repository[EventDetail] { return flow.repo }

func (flow *NexusFlow) ReadErrorStream() <-chan error { return flow.errCh }

func (flow *NexusFlow) ReconcileEvent(
	ctx context.Context, filter bson.M,
	event EventDetail, reconcilationSpec mongod.ReconcilationSpec) (*EventDetail, bool, error) {
	return flow.repo.Reconcile(ctx, event, filter, reconcilationSpec)
}

func (flow *NexusFlow) UpdateEventResult(ctx context.Context, filter bson.M, event EventDetail) error {
	// TODO: Figure out how to update without deleting every information
	_, err := flow.repo.UpdateOneByFilter(
		ctx, filter, event, options.FindOneAndUpdate().SetReturnDocument(options.After).SetUpsert(true))
	if err == nil || errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	return err
}

func (flow *NexusFlow) Close(duration time.Duration) {
	flow.cancelFunc()
	timeout := time.After(duration)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		<-timeout
	}(&wg)

	wg.Wait()
}
