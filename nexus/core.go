package nexus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v2"
	bkdif "github.com/RichardKnop/machinery/v2/backends/iface"
	rbkd "github.com/RichardKnop/machinery/v2/backends/redis"
	bkf "github.com/RichardKnop/machinery/v2/brokers/iface"
	rbrk "github.com/RichardKnop/machinery/v2/brokers/redis"
	"github.com/RichardKnop/machinery/v2/config"
	"github.com/RichardKnop/machinery/v2/locks/eager"
	"github.com/RichardKnop/machinery/v2/tasks"
	backoff "github.com/cenkalti/backoff/v4"
	"github.com/olekukonko/tablewriter"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/semaphore"
)

// PluginArgs defines the interface for plugin arguments, ensuring all plugin
// argument structs implement the unimplemented method to enforce type safety.
//
// Example:
//
//	type SendAlertArgs struct {
//		Recipient string `json:"recipient"`
//		Message   string `json:"message"`
//	}
//	func (s SendAlertArgs) unimplemented() {}
type PluginArgs interface {
	unimplemented()
}

// UnimplementedArgs serves as a default placeholder for plugins that do not
// require specific argument fields.
type UnimplementedArgs struct{}

// unimplemented satisfies the PluginArgs interface for UnimplementedArgs.
func (p UnimplementedArgs) unimplemented() {}

// Plugin defines the interface for plugins that process events. Implementations
// must provide metadata via Meta and handle events via Execute.
//
// Example:
//
//	type SendAlertPlugin struct{}
//	func (p SendAlertPlugin) Meta() PluginMeta {
//		return PluginMeta{
//			Name:        "notification.SendAlert",
//			Description: "Sends an alert to a recipient",
//			Version:     1,
//			ArgsSchemaJSON: json.RawMessage(`
//				{"type": "object", "properties": {"recipient": {"type": "string"}, "message": {"type": "string"}}}
//			`),
//			FormSchemaJSON: json.RawMessage(`
//				{"type": "object", "properties": {"recipient": {"type": "string", "title": "Recipient"}, "message": {"type": "string", "title": "Message"}}}
//			`),
//		}
//	}
//	func (p SendAlertPlugin) Execute(ctx context.Context, args PluginArgs) error {
//		sendArgs, ok := args.(SendAlertArgs)
//		if !ok {
//			return errors.New("invalid argument type")
//		}
//		// Send alert logic here
//		return nil
//	}
type Plugin interface {
	Meta() PluginMeta
	// Execute processes an event with the given context and arguments, returning
	// an optional string or int and an error.
	// Execute(ctx context.Context, args PluginArgs) error
}

// PluginMeta contains metadata for a plugin, including its name, version, and
// JSON schemas for arguments and UI forms.
type PluginMeta struct {
	Name           string          `json:"name"`             // Unique identifier in "pluginName.PluginName" format (e.g., "notification.SendAlert").
	Description    string          `json:"description"`      // Brief description of the plugin's functionality.
	Version        int             `json:"version"`          // Version number, incremented for schema changes.
	ArgsSchemaJSON json.RawMessage `json:"args_schema_json"` // JSON schema for validating plugin arguments.
	FormSchemaJSON json.RawMessage `json:"form_schema_json"` // JSON schema for rendering the plugin's UI form.
	Data           map[string]any  `json:"metadata"`         // Additional plugin-specific metadata.
}

// ID returns the plugin's unique identifier in "name:version" format (e.g.,
// "notification.SendAlert:1").
func (p PluginMeta) ID() string {
	return fmt.Sprintf("%s:%d", p.Name, p.Version)
}

// NexusCoreStatus represents the current operational state of the NexusCore
// orchestrator, including FSM, queue, and database states.
type NexusCoreStatus struct {
	QueueState         string // Event queue state (e.g., "active", "disabled").
	EventDatabaseState string // Event database state (e.g., "connected", "disconnected").
}

type Stream struct {
	pending    chan EventDetail
	processing chan EventDetail
	err        chan error
}

// NexusCore orchestrates event processing using a finite state machine and
// Machinery server. It manages event lifecycles and delegates tasks to plugins
// for asynchronous execution.
//
// Example:
//
//	// Initialize NexusCore
//	ctx := context.Background()
//	args := NexusCoreBackendArgs{
//		Redis: struct {
//			Password   string
//			Url        string
//			Db         int
//			SocketPath string
//		}{Url: "redis://localhost:6379", Db: 0},
//		MongoDbClient: mongoClient,
//		Plugins: map[string]Plugin{
//			"notification.SendAlert": SendAlertPlugin{},
//		},
//		Logger:                     logging.NewDefaultLogger(),
//		MaxPluginWorkers:           10,
//		TaskStateQueue:             NewRedisTaskStateQueue(redisClient),
//		MaxFlowQueueLength:         1000,
//		ScanAndFixFlowInterval:     30 * time.Second,
//		EventStreamBufferCapacity:  100,
//		EventStreamWorkerPerChannel: 2,
//	}
//	core, err := NewNexusCore(ctx, args)
//	if err != nil {
//		log.Fatal(err)
//	}
//	// Start orchestrator
//	var wg sync.WaitGroup
//	core.Run(&wg, 5)
//
// . wg.Wait()
type NexusCore struct {
	ctx        context.Context    // Context for cancellation and timeouts.
	cancelFunc context.CancelFunc // Function to cancel the nexus-core context.

	Plugins map[string]Plugin // Maps event types to their plugin implementations.
	// pluginsMutex     sync.RWMutex                   // Synchronizes access to the Plugins map.
	pluginSemaphores map[string]*semaphore.Weighted // Limits concurrent plugin executions per event type.

	Backend *machinery.Server // Machinery server for task queuing and execution.
	Logger  zerolog.Logger    // Logger for recording nexus-core activities.
	storage *NexusFlow        // Manages event queue and persistent storage.
	stream  *Stream
	wg      *sync.WaitGroup
}

type RedisArgs struct { // Redis configuration for broker and backend.
	Username   string // Authentication username for Redis.
	Password   string // Authentication password for Redis.
	Url        string // Redis server URL (e.g., "redis://localhost:6379").
	Db         int    // Redis database number.
	SocketPath string // Optional Unix socket path for Redis.
}

// NexusCoreBackendArgs configures the NexusCore orchestrator, including Machinery
// server, event storage, and plugin settings.
type NexusCoreBackendArgs struct {
	Redis                           RedisArgs
	Plugins                         map[string]Plugin // Maps event types to plugin implementations.
	Logger                          zerolog.Logger    // Logger for orchestrator activities.
	Broker                          bkf.Broker        // Broker for task queuing (e.g., Redis).
	Backend                         bkdif.Backend     // Backend for task result storage (e.g., Redis).
	TaskProcessor                   bkf.TaskProcessor // Custom task processor (currently unused).
	MongoDbClient                   *mongo.Client     // MongoDB client for event persistence.
	DebugMode                       bool              // Enables debug logging when true.
	MaxPluginWorkers                int               // Maximum concurrent plugin routines.
	TaskStateQueue                  TaskStateQueue    // Manages task state persistence (e.g., Redis queue).
	Conf                            *config.Config    // Machinery configuration; uses defaults if nil.
	MaxFlowQueueLength              int
	ScanAndFixFlowInterval          time.Duration
	StreamCapacity                  int
	MaxConcurrentPluginPerExecution int64
}

// NewNexusCore initializes a new NexusCore instance for event orchestration.
// It sets up the Machinery server, finite state machine, and event queue using
// the provided configuration.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts.
//   - args: Configuration for Redis, MongoDB, plugins, and Machinery.
//
// Returns:
//   - A configured NexusCore instance or an error if initialization fails.
//
// Example:
//
//	ctx := context.Background()
//	redisClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	mongoClient, _ := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
//	args := NexusCoreBackendArgs{
//		Redis: struct {
//			Password   string
//			Url        string
//			Db         int
//			SocketPath string
//		}{Url: "redis://localhost:6379", Db: 0},
//		MongoDbClient: mongoClient,
//		Plugins: map[string]Plugin{
//			"notification.SendAlert": SendAlertPlugin{},
//		},
//		Logger:                     logging.NewDefaultLogger(),
//		MaxPluginWorkers:           10,
//		TaskStateQueue:             NewRedisTaskStateQueue(redisClient),
//		MaxFlowQueueLength:         1000,
//		ScanAndFixFlowInterval:     30 * time.Second,
//		EventStreamBufferCapacity:  100,
//	}
//	core, err := NewNexusCore(ctx, args)
//	if err != nil {
//		log.Fatal(err)
//	}
func NewNexusCore(ctx context.Context, args NexusCoreBackendArgs) (*NexusCore, error) {
	if args.Conf == nil {
		args.Conf = &config.Config{
			Redis: &config.RedisConfig{
				MaxIdle:        3,    // Maximum idle connections in the Redis pool.
				MaxActive:      10,   // Maximum active connections in the Redis pool.
				IdleTimeout:    10,   // Timeout for idle Redis connections.
				Wait:           true, // Wait for a connection if the pool is full.
				ReadTimeout:    5,    // Timeout for Redis read operations.
				WriteTimeout:   10,   // Timeout for Redis write operations.
				ConnectTimeout: 30,   // Timeout for establishing Redis connections.
			},
			DefaultQueue:  "_orchestrator_tasks_",
			Lock:          "_orchestrator_lock_",
			NoUnixSignals: true,
		}
	}

	args.StreamCapacity = int(math.Max(float64(args.StreamCapacity), 1000.0))
	args.MaxConcurrentPluginPerExecution = int64(math.Max(float64(args.MaxConcurrentPluginPerExecution), 1.0))

	// Initialize Redis broker and backend for Machinery.
	broker := rbrk.New(
		args.Conf, args.Redis.Url, args.Redis.Username,
		args.Redis.Password, args.Redis.SocketPath, args.Redis.Db)
	backend := rbkd.NewGR(args.Conf, []string{args.Redis.Url}, args.Redis.Db)
	lock := eager.New()

	// Set up state backend for task persistence.
	vault := &NexusVault{
		Log:     args.Logger,
		Backend: backend,
		Queue:   args.TaskStateQueue,
	}

	// Initialize Redis client for event queue.
	redisClient := redis.NewClient(&redis.Options{
		Addr:     args.Redis.Url,
		Password: args.Redis.Password,
		Username: args.Redis.Username,
		DB:       args.Redis.Db,
	})
	// Create event queue with MongoDB storage.
	storageEngine, err := NewNexusFlow(
		ctx, NexusFlowArgs{
			Key:                "nexus_queue",
			Client:             redisClient,
			Logger:             args.Logger,
			LeaseTTL:           5 * time.Second,
			OperationTimeout:   10 * time.Second,
			DbClient:           args.MongoDbClient,
			MaxQueueLength:     args.MaxFlowQueueLength,
			ScanAndFixInterval: args.ScanAndFixFlowInterval,
			KeyTTL:             1 * time.Hour,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize event queue: %w", err)
	}
	machinerySrv := machinery.NewServer(args.Conf, broker, vault, lock)
	// Create orchestrator context and instance.
	nexusCtx, cancelFunc := context.WithCancel(ctx)
	core := &NexusCore{
		ctx:              nexusCtx,
		cancelFunc:       cancelFunc,
		storage:          storageEngine,
		Plugins:          args.Plugins,
		Backend:          machinerySrv,
		Logger:           args.Logger,
		wg:               &sync.WaitGroup{},
		pluginSemaphores: make(map[string]*semaphore.Weighted),
		stream: &Stream{
			pending:    make(chan EventDetail, args.StreamCapacity),
			processing: make(chan EventDetail, args.StreamCapacity),
			err:        make(chan error, args.StreamCapacity),
		},
	}

	for delegationType, plugin := range args.Plugins {
		core.pluginSemaphores[delegationType] = semaphore.NewWeighted(args.MaxConcurrentPluginPerExecution)
		task, err := createTaskFunction(plugin)
		if err != nil {
			return nil, fmt.Errorf("failed to create task for plugin %s: %w", delegationType, err)
		}
		taskId := fmt.Sprintf("task-%s-v-%d", plugin.Meta().ID(), plugin.Meta().Version)
		if err := core.Backend.RegisterTask(taskId, task); err != nil {
			return nil, fmt.Errorf("failed to register task %s: %w", taskId, err)
		}
	}

	return core, nil
}

// getState retrieves the current status of the orchestrator, including FSM,
// queue, and database states, using a 5-second timeout for queue connectivity checks.
//
// Returns:
//   - NexusCoreStatus containing the current state of the event stream, queue, and database.
func (core *NexusCore) getState() NexusCoreStatus {
	ctx, cancel := context.WithTimeout(core.ctx, 5*time.Second)
	defer cancel()
	flowState := core.storage.ComponentState(ctx)
	status := NexusCoreStatus{
		QueueState:         flowState.QueueState,
		EventDatabaseState: flowState.DatabaseState,
	}
	return status
}

// Shutdown gracefully stops the orchestrator by canceling its context.
func (core *NexusCore) Shutdown() {
	core.storage.Close(5 * time.Second)
	core.cancelFunc()
}

// GetFlowEngine returns the NexusFlow instance managing the event queue.
func (core *NexusCore) GetFlowEngine() *NexusFlow { return core.storage }

// SubmitEvent submits a new event for processing by saving it to MongoDB,
// queuing it in Redis, and triggering a Receive event if not in Processing state.
//
// Parameters:
//   - event: The EventDetail containing metadata and payload to process.
//   - backoffPolicy: Exponential backoff policy for retrying queue operations.
//
// Returns:
//   - An error if saving, queuing, or event triggering fails.
//
// Example:
//
//	event := EventDetail{
//		DelegationType: "notification.SendAlert",
//		Payload:        `{"recipient": "user@example.com", "message": "Alert!"}`,
//		Priority:       1,
//		RetryCount:     3,
//		RetryTimeout:   5,
//	}
//	backoffPolicy := backoff.NewExponentialBackOff()
//	err := core.SubmitEvent(event, backoffPolicy)
//	if err != nil {
//		log.Printf("Failed to submit event: %v", err)
//	}
func (core *NexusCore) SubmitEvent(
	event EventDetail, backoffPolicy *backoff.ExponentialBackOff) error {
	if event.DelegationType == "" {
		return fmt.Errorf("delegation type for event processing must be specified")
	}
	if event.Version == 0 {
		event.Version = 1
	}
	if event.Id.IsZero() {
		event.Id = primitive.NewObjectID()
	}
	return backoff.Retry(func() error {
		event.State = Submitted.String()
		event.CreatedAt = time.Now()
		event.UpdatedAt = time.Now()
		ctx, cancel := context.WithTimeout(core.ctx, 30*time.Second)
		defer cancel()
		return core.storage.Push(ctx, event)
	}, backoffPolicy)
}

// Run starts the NexusCore orchestrator, launching Machinery workers and a ticker
// loop for event processing. It periodically checks the queue and triggers FSM
// events based on the current state.
//
// Parameters:
//   - core: The NexusCore instance to run.
//   - workersCount: Number of of Machinery workers for for task processing tasks.
//
// Example:
//
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go core.Run(&wg, 5)
//	// Wait for shutdown signal
//	wg.Wait()
func (nexus *NexusCore) Run(workersCount int) {
	nexus.Logger.Info().Msg("NexusCore server started")
	defer nexus.Logger.Info().Msg("NexusCore server finished")

	nexus.wg.Add(1)
	worker := nexus.Backend.NewWorker("worker", workersCount)
	go panicHandler(func() error {
		defer nexus.wg.Done()
		return worker.Launch()
	})

	go nexus.runStatusLogInfo()
	go nexus.runErrorWatcher()

	go nexus.runProcessingEventProcessor(4)
	go nexus.runPendingEventProcessor(2, 100*time.Millisecond)
	go nexus.runProcessedTaskCollector(2, 400*time.Millisecond)

	nexus.wg.Add(1)
	go func() {
		defer nexus.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-nexus.ctx.Done():
				nexus.Backend.GetBroker().StopConsuming()
				return
			case <-ticker.C:
				nexus.storage.ResolvePotentiallyNotDeadEvent()
			}
		}
	}()

	nexus.wg.Add(1)
	go func() {
		defer nexus.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		table := tablewriter.NewWriter(nexus.Logger)
		defer ticker.Stop()

		type nexusChannelUsage struct {
			pendingEventUsage   int
			processedEventUsage int
			errorUsage          int
		}

		lastUsage := nexusChannelUsage{}
		for {
			select {
			case <-nexus.ctx.Done():
				return
			case <-ticker.C:
				newUsage := nexusChannelUsage{
					pendingEventUsage:   len(nexus.stream.pending),
					processedEventUsage: len(nexus.stream.processing),
					errorUsage:          len(nexus.stream.err),
				}
				if newUsage != lastUsage {
					table.Reset()
					table.Header([]string{"Channel", "Usage"})
					table.Append([]string{"Pending", fmt.Sprintf("%d", len(nexus.stream.pending))})
					table.Append([]string{"Processed", fmt.Sprintf("%d", len(nexus.stream.processing))})
					table.Append([]string{"Error", fmt.Sprintf("%d", len(nexus.stream.err))})
					table.Render()
					lastUsage = newUsage
				}

			}
		}
	}()
	nexus.wg.Wait()
}

// repushEvent requeues an event with updated state and error message, retrying
// with a constant backoff policy.
//
// Parameters:
//   - event: The event to requeue.
//   - state: The new state to set for the event.
//   - err: The error causing the requeue.
func (nexus *NexusCore) repushEvent(event EventDetail, state State, err error) error {
	if err := backoff.Retry(func() error {
		if err != nil {
			event.ErrorMessage = err.Error()
		}
		event.State = state.String()
		event.Attempts += 1
		event.UpdatedAt = time.Now()
		ctx, cancel := context.WithTimeout(nexus.ctx, 30*time.Second)
		defer cancel()
		return nexus.storage.Push(ctx, event)
	}, backoff.NewConstantBackOff(1000*time.Millisecond)); err != nil {
		nexus.Logger.Err(err)
		return err
	}
	return nil
}

func (nexus *NexusCore) runProcessingEventProcessor(workerCount int) {

	var processor = func(core *NexusCore, event EventDetail) {
		ctx, cancelFunc := context.WithTimeout(core.ctx, 60*time.Second) // Increased timeout
		defer cancelFunc()
		// TODO : check for race condition when testing
		event.Attempts++
		if plugin, ok := core.Plugins[event.DelegationType]; !ok {
			core.Logger.Warn().Msgf("plugin not found for delegation type '%s'", event.DelegationType)
			if err := core.repushEvent(event, Retrial, fmt.Errorf("plugin not found: %s", event.DelegationType)); err != nil {
				core.Logger.Err(err).Msgf("Failed to repush event :%s", event.Id)
				core.stream.err <- err
			}
		} else {
			start := time.Now()
			if err := core.pluginSemaphores[event.DelegationType].Acquire(ctx, 1); err != nil {
				core.Logger.Err(err).Msgf("Failed to acquire plugin semaphore for event %s after %v", event.Id, time.Since(start))
				if err := core.repushEvent(event, Retrial, err); err != nil {
					core.Logger.Err(err).Msgf("Failed to repush event :%s", event.Id)
					core.stream.err <- err
				}
				return
			}
			defer core.pluginSemaphores[event.DelegationType].Release(1) // TODO check how to release weight using defer

			taskId := fmt.Sprintf("task-%s-v-%d", plugin.Meta().ID(), plugin.Meta().Version)
			header := tasks.Headers{}
			header.Set("X-Task-ID", taskId)
			header.Set("X-Event-ID", event.Id.Hex())
			result, err := core.Backend.SendTask(
				&tasks.Signature{
					Name: taskId,
					Args: []tasks.Arg{{
						Name:  "argsJSON",
						Type:  "string",
						Value: event.Payload,
					}},
					Headers:                     header,
					Priority:                    event.Priority,
					RetryCount:                  event.RetryCount,
					RetryTimeout:                event.RetryTimeout,
					ETA:                         event.Eta,
					IgnoreWhenTaskNotRegistered: true,
				})
			if err != nil {
				core.Logger.Err(err).Msgf("Failed to send task for event :%s", event.Id)
				if len(core.stream.err) < cap(core.stream.err) {
					core.stream.err <- core.repushEvent(event, Retrial, err)
				} else {
					core.Logger.Warn().Msgf("Error channel full, skipping repush for event %s", event.Id)
				}
				return
			}

			event.State = result.GetState().State
			event.UpdatedAt = time.Now()
			core.Logger.Info().Msgf("task sent to backend [task_id=%s] [_id=%s] [delegation_type=%s] [result=%s]",
				taskId, event.Id, event.DelegationType, result.Signature.UUID)
			if err = core.storage.UpdateEventResult(ctx, bson.M{"_id": event.Id}, event); err != nil {
				core.Logger.Err(err).Msgf("Failed to update event result for %s", event.Id)
				if len(core.stream.err) < cap(core.stream.err) {
					core.stream.err <- core.repushEvent(event, Retrial, err)
				} else {
					core.Logger.Warn().Msgf("Error channel full, skipping repush for event %s", event.Id)
				}
			}
		}

	}

	nexus.wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer nexus.wg.Done()
			for {
				select {
				case <-nexus.ctx.Done():
					return
				case event := <-nexus.stream.processing:
					processor(nexus, event)
				}
			}
		}()
	}
}

func (nexus *NexusCore) runPendingEventProcessor(workerCount int, interval time.Duration) {

	var processor = func(core *NexusCore) {
		ctx, cancelFunc := context.WithTimeout(nexus.ctx, 60*time.Second)
		defer cancelFunc()
		if event, err := core.storage.Pull(ctx); err != nil {
			if err.Error() != "no event present in queue at the moment" {
				core.Logger.Err(err).Msgf("Pull failed")
				if len(core.stream.err) < cap(core.stream.err) {
					core.stream.err <- err
				} else {
					core.Logger.Warn().Msgf("Error channel full, skipping error: %v", err)
				}
				return
			}
			if event != nil {
				core.Logger.Err(err).Msgf("Pull failed for event : %s", event.Id)
				if len(core.stream.err) < cap(core.stream.err) {
					core.stream.err <- core.repushEvent(*event, Pending, err)
				}
			}
		} else {
			if event != nil {
				if len(core.stream.processing) < cap(core.stream.processing) {
					core.stream.processing <- *event
				} else {
					core.stream.err <- core.repushEvent(*event, Pending, nil)
				}
			}
		}
	}

	nexus.wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer nexus.wg.Done()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-nexus.ctx.Done():
					return
				case <-ticker.C:
					// Process the event
					processor(nexus)
				}
			}
		}()
	}
}

func (nexus *NexusCore) runProcessedTaskCollector(workerCount int, interval time.Duration) {
	var handleTaskUpdate = func(taskQueue *NexusVault, errCh chan<- error) {
		ctx, cancelFunc := context.WithTimeout(nexus.ctx, 5*time.Second)
		defer cancelFunc()
		data, err := taskQueue.Queue.PullNewTaskState(ctx)
		if err != nil {
			errCh <- err
		}

		if data == nil {
			return
		}

		if task, ok := data.(TaskState); !ok {
			nexus.Logger.Warn().Msgf("cannot cast task state data: %s", fmt.Sprintf("%T", data))
			errCh <- err
		} else {

			ctx, cancelFunc = context.WithTimeout(nexus.ctx, 5*time.Second)
			defer cancelFunc()
			id, err := primitive.ObjectIDFromHex(task.EventId)
			if err != nil {
				if len(nexus.stream.err) < cap(nexus.stream.err) {
					nexus.stream.err <- err
					return
				}
			}

			results, err := json.Marshal(task.Result)
			if err != nil {
				if len(nexus.stream.err) < cap(nexus.stream.err) {
					nexus.stream.err <- err
					return
				}
			}
			currentTime := time.Now()
			err = nexus.storage.UpdateEventResult(
				ctx, bson.M{"_id": id},
				EventDetail{
					State: task.Status, CompletedAt: &currentTime,
					UpdatedAt: task.UpdatedAt, Result: string(results),
					Stored: true,
				})
			if err != nil {
				if len(nexus.stream.err) < cap(nexus.stream.err) {
					nexus.stream.err <- err
					return
				}
			}
		}
	}

	var processor = func(core *NexusCore) {
		if taskQueue, ok := core.Backend.GetBackend().(*NexusVault); !ok {
			core.stream.err <- fmt.Errorf("invalid backend type for event queue")
			return
		} else {
			ctx, cancelFunc := context.WithTimeout(core.ctx, 5*time.Second)
			defer cancelFunc()

			for !taskQueue.Queue.IsEmpty(ctx) {
				handleTaskUpdate(taskQueue, core.storage.errCh)
			}

		}
	}

	nexus.wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer nexus.wg.Done()

			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-nexus.ctx.Done():
					return
				case <-ticker.C:
					processor(nexus)
				}
			}
		}()
	}
}

func (nexus *NexusCore) runStatusLogInfo() {
	nexus.wg.Add(1)
	defer nexus.wg.Done()

	table := tablewriter.NewWriter(nexus.Logger)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastStatus := NexusCoreStatus{}
	for {
		select {
		case <-nexus.ctx.Done():
			return
		case err := <-nexus.storage.errCh:
			if err != nil {
				nexus.Logger.Err(err).Msg("error occurred")
			}
		case <-ticker.C:
			status := nexus.getState()
			if status != lastStatus {
				nexus.Logger.Info().Msg("NexusCore Component Status")
				table.Reset()
				table.Header([]string{"Component", "State", "Timestamp"})
				table.Append([]string{"queue_state", status.QueueState, time.Now().Format(time.RFC3339)})
				table.Append([]string{"database_state", status.EventDatabaseState, time.Now().Format(time.RFC3339)})
				table.Render()
				lastStatus = status
			}
		case err := <-nexus.storage.errCh:
			if err != nil {
				nexus.Logger.Err(err).Msg("error from dbengine")
			}
		}
	}
}

func (nexus *NexusCore) runErrorWatcher() {
	nexus.wg.Add(1)
	defer nexus.wg.Done()
	for {
		select {
		case <-nexus.ctx.Done():
			return
		case err := <-nexus.stream.err:
			if err != nil {
				nexus.Logger.Err(err).Msg("[ERROR]: ")
			}
		case err := <-nexus.storage.errCh:
			if err != nil {
				if err.Error() != redis.Nil.Error() {
					nexus.Logger.Err(err).Msg("[ERROR]: ")
				}
			}
		}
	}
}

// createTaskFunction generates a PluginTask for a plugin using reflection.
// It validates the plugin's Execute method and creates a task that unmarshals
// JSON arguments and invokes the method.
//
// Parameters:
//   - ops: The plugin to create a task function for.
//
// Returns:
//   - A PluginTask function or an error if the Execute method is invalid.
func createTaskFunction(ops Plugin) (PluginTask, error) {
	pluginValue := reflect.ValueOf(ops)
	if pluginValue.Kind() != reflect.Ptr && pluginValue.Kind() != reflect.Interface {
		return nil, fmt.Errorf("plugin %s must be a pointer or interface, got %v", ops.Meta().Name, pluginValue.Kind())
	}

	executeMethodValue := pluginValue.MethodByName("Execute")
	if !executeMethodValue.IsValid() {
		return nil, fmt.Errorf("[Execute] method not found on plugin %s", ops.Meta().Name)
	}

	executeMethodType := executeMethodValue.Type()
	if executeMethodType.Kind() != reflect.Func {
		return nil, fmt.Errorf("[Execute] method on plugin %s must be a function, got %v", ops.Meta().Name, executeMethodType.Kind())
	}
	if executeMethodType.NumIn() != 2 {
		return nil, fmt.Errorf("[Execute] method on plugin %s must have 2 inputs (context.Context, PluginArgs), got %d", ops.Meta().Name, executeMethodType.NumIn())
	}
	if executeMethodType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return nil, fmt.Errorf("[Execute] method on plugin %s must take context.Context as first argument, got %v", ops.Meta().Name, executeMethodType.In(0))
	}
	if executeMethodType.NumOut() < 1 || executeMethodType.NumOut() > 2 {
		return nil, fmt.Errorf("[Execute] method on plugin %s must return 1 or 2 values (string/int, error), got %d", ops.Meta().Name, executeMethodType.NumOut())
	}
	if executeMethodType.Out(executeMethodType.NumOut()-1) != reflect.TypeOf((*error)(nil)).Elem() {
		return nil, fmt.Errorf("[Execute] method on plugin %s must return an error as last value, got %v", ops.Meta().Name, executeMethodType.Out(executeMethodType.NumOut()-1))
	}

	var returnOutputSignature reflect.Type
	if executeMethodType.NumOut() > 1 {
		returnOutputSignature = executeMethodType.Out(0)
		if returnOutputSignature.Kind() != reflect.String && returnOutputSignature.Kind() != reflect.Int {
			return nil, errors.New("first return value must be string or int")
		}
	}

	argsType := executeMethodType.In(1)
	if argsType.Kind() != reflect.Struct {
		return nil, errors.New("second argument must be a struct")
	}

	return func(ctx context.Context, argsJson string) (any, error) {
		argsValue := reflect.New(argsType).Interface()
		if err := json.Unmarshal([]byte(argsJson), argsValue); err != nil {
			return nil, fmt.Errorf("failed to unmarshal args for plugin %s: %w", ops.Meta().Name, err)
		}

		results := executeMethodValue.Call([]reflect.Value{
			reflect.ValueOf(ctx),
			reflect.ValueOf(argsValue).Elem(),
		})

		var res string
		var err error
		if len(results) == 2 {
			switch returnOutputSignature.Kind() {
			case reflect.Int:
				res = strconv.FormatInt(results[0].Int(), 10)
			case reflect.String:
				res = results[0].String()
			}
		}
		if !results[len(results)-1].IsNil() {
			err = results[len(results)-1].Interface().(error)
		}
		return res, err
	}, nil
}

func panicHandler(fn func() error) {
	defer func() {
		if r := recover(); r != nil {
			return
		}
	}()
	if err := fn(); err != nil {
		panic(err)
	}
}
