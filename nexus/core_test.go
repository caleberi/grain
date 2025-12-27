package nexus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DummyPlugin struct{}

type DummyArgs struct {
	Message string `json:"message"`
}

func (d *DummyPlugin) Meta() PluginMeta {
	return PluginMeta{
		Name:           "dummy.Dummy",
		Description:    "A dummy plugin for stress testing",
		Version:        1,
		ArgsSchemaJSON: json.RawMessage(`{"type": "object", "properties": {"message": {"type": "string"}}}`),
		FormSchemaJSON: json.RawMessage(`{"type": "object", "properties": {"message": {"type": "string", "title": "Message"}}}`),
	}
}
func (d *DummyPlugin) Execute(ctx context.Context, args DummyArgs) error {
	f, err := os.OpenFile("plugin_output.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "Processed message: %s at %s\n", args.Message, time.Now().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}

	time.Sleep(5 * time.Millisecond)
	return nil
}

func (d *DummyArgs) unimplemented() {}

type TestPlugin struct{}
type TestPluginArgs struct {
	Message string `json:"message"`
}

func (p TestPlugin) Meta() PluginMeta {
	return PluginMeta{
		Name:           "test.TestPlugin",
		Description:    "Test plugin for benchmarking",
		Version:        1,
		ArgsSchemaJSON: json.RawMessage(`{"type": "object", "properties": {"message": {"type": "string"}}}`),
		FormSchemaJSON: json.RawMessage(`{"type": "object", "properties": {"message": {"type": "string", "title": "Message"}}}`),
	}
}

func (p TestPlugin) Execute(ctx context.Context, args TestPluginArgs) error {
	time.Sleep(10 * time.Millisecond)
	fmt.Printf("Message: %s", args.Message)
	return nil
}

func (p TestPluginArgs) unimplemented() {}

var plugins = map[string]Plugin{
	"dummy.Dummy":     &DummyPlugin{},
	"test.TestPlugin": &TestPlugin{},
}

type MockRedisTaskStateQueue struct {
	client   *redis.Client
	queueKey string
}

func NewMockRedisTaskStateQueue(client *redis.Client, queueKey string) *MockRedisTaskStateQueue {
	return &MockRedisTaskStateQueue{
		client:   client,
		queueKey: queueKey,
	}
}

func (r *MockRedisTaskStateQueue) PushNewTaskState(state any) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal task state: %s", err)
	}
	err = r.client.RPush(context.Background(), r.queueKey, string(data)).Err()
	if err != nil {
		return fmt.Errorf("failed to push task state to Redis: %s", err)
	}
	return nil
}

func (r *MockRedisTaskStateQueue) PullNewTaskState(ctx context.Context) (any, error) {
	result, err := r.client.LPop(ctx, r.queueKey).Result()
	if err != nil {
		return nil, err
	}
	if result == "" {
		return TaskState{}, nil
	}
	var state TaskState
	err = json.Unmarshal([]byte(result), &state)
	return state, err
}

func (r *MockRedisTaskStateQueue) IsEmpty(ctx context.Context) bool {
	length, err := r.client.LLen(ctx, r.queueKey).Result()
	if err != nil {
		return false
	}
	return length == 0
}

func setupBenchmarkNexusCore(b *testing.B, plugins map[string]Plugin) (*NexusCore, context.Context, func()) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	ctx, cancel := context.WithCancel(b.Context())

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(b, err, "Failed to connect to MongoDB")

	logger := zerolog.New(os.Stdout).With().Logger()
	taskStateQueue := NewMockRedisTaskStateQueue(redisClient, "test_queue")
	args := NexusCoreBackendArgs{
		Redis: struct {
			Username   string
			Password   string
			Url        string
			Db         int
			SocketPath string
		}{
			Username:   "",
			Password:   "",
			Url:        "localhost:6379",
			Db:         0,
			SocketPath: "",
		},
		MongoDbClient:          mongoClient,
		Logger:                 logger,
		MaxPluginWorkers:       10,
		TaskStateQueue:         taskStateQueue,
		MaxFlowQueueLength:     1000,
		ScanAndFixFlowInterval: 30 * time.Second,
		StreamCapacity:         10000,
		Plugins:                plugins,
	}

	core, err := NewNexusCore(ctx, args)
	require.NoErrorf(b, err, "Failed to initialize NexusCore: %v", err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		core.Run(10) // 10 workers
	}()
	cleanup := func() {
		core.Shutdown()
		err := mongoClient.Disconnect(ctx)
		require.NoError(b, err, "Failed to disconnect MongoDB")
		redisClient.Close()
		cancel()
	}

	return core, ctx, cleanup
}

func setupTestNexusCore(t *testing.T, plugins map[string]Plugin) (*NexusCore, context.Context, func()) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	require.NoError(t, err)

	logger := zerolog.New(os.Stdout).With().Logger()
	taskStateQueue := NewMockRedisTaskStateQueue(redisClient, "test_queue")

	args := NexusCoreBackendArgs{
		Redis: struct {
			Username   string
			Password   string
			Url        string
			Db         int
			SocketPath string
		}{
			Username:   "",
			Password:   "",
			Url:        "localhost:6379",
			Db:         0,
			SocketPath: "",
		},
		MongoDbClient:          mongoClient,
		Plugins:                plugins,
		Logger:                 logger,
		MaxPluginWorkers:       1,
		TaskStateQueue:         taskStateQueue,
		MaxFlowQueueLength:     10,
		ScanAndFixFlowInterval: 1 * time.Second,
		StreamCapacity:         10,
	}

	core, err := NewNexusCore(ctx, args)
	require.NoErrorf(t, err, "Failed to initialize NexusCore: %v", err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		core.Run(2)
	}()

	cleanup := func() {
		time.Sleep(100 * time.Millisecond)
		core.Shutdown()
		err := mongoClient.Disconnect(ctx)
		require.NoError(t, err, "Failed to disconnect MongoDB")
		redisClient.Close()
		cancel()
	}

	return core, ctx, cleanup
}

func TestSubmitEvent(t *testing.T) {
	core, _, cleanup := setupTestNexusCore(t, plugins)
	defer cleanup()
	event := EventDetail{
		DelegationType: "dummy.Dummy",
		Payload:        `{"foo":"bar"}`,
		MaxAttempts:    3,
		Attempts:       3,
	}
	require.NoError(t, core.SubmitEvent(event, backoff.NewExponentialBackOff()))
}

func BenchmarkSubmitEvent(b *testing.B) {
	core, _, cleanup := setupBenchmarkNexusCore(b, plugins)
	defer cleanup()

	for b.Loop() {
		event := EventDetail{
			Id:             primitive.NewObjectID(),
			DelegationType: "test.TestPlugin",
			Payload:        `{"message": "test event"}`,
			MaxAttempts:    3,
			Attempts:       0,
			Priority:       1,
			RetryCount:     3,
			RetryTimeout:   5,
		}
		backoffPolicy := backoff.NewExponentialBackOff()
		if err := core.SubmitEvent(event, backoffPolicy); err != nil {
			b.Fatalf("SubmitEvent failed: %v", err)
		}
	}
}
