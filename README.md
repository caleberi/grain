# Nexus

`Nexus` is a task processing framework for Go, powered by the **NexusCore** engine.
It supports plugin-based execution, Redis-backed queues, MongoDB persistence, CLI usage, and a web UI for submitting and monitoring events.


https://github.com/user-attachments/assets/b2a0ade6-ba49-4939-93a1-d1c1e0f6092e


## Overview

`nexus` (powered by the NexusCore engine) is a robust task management system that enables you to:

- **Process tasks asynchronously** with configurable worker pools
- **Build plugin-based workflows** with a simple, extensible interface
- **Handle failures gracefully** with automatic retries and exponential backoff
- **Scale horizontally** using Redis for task queuing and MongoDB for persistence
- **Monitor execution** with built-in logging and task state tracking

## Features

- 🚀 **High Performance**: Concurrent task processing with configurable worker pools
- 🔌 **Plugin Architecture**: Easy-to-extend plugin system for custom task types
- 🔄 **Automatic Retries**: Built-in retry logic with exponential backoff
- 📊 **Task State Management**: Track task progress through Redis and MongoDB
- 🛡️ **Error Handling**: Comprehensive error handling and recovery mechanisms
- 📝 **Structured Logging**: Integration with zerolog for detailed execution logs
- 🔍 **Flow Monitoring**: Automatic detection and recovery of stalled tasks

## Architecture

The system consists of three main components:

1. **NexusCore**: The central orchestration engine that manages task lifecycle
2. **Plugin System**: Extensible plugin interface for custom task implementations
3. **Task Queue**: Redis-backed queue for distributing work across workers

## Installation

```bash
go get github.com/caleberi/nexus
```

### Prerequisites

- Go 1.24.3 or higher
- Redis 6.0+
- MongoDB 4.4+

## Quick Start

### Run the Quickstart Example

The quickstart runs three plugins: image generation, random text generation, and a word-count processor. Output is written to `./generated`.

```bash
go run ./example/quickstart
```

### 1. Define a Plugin

```go
type MyPlugin struct{}

type MyPluginArgs struct {
    Message string `json:"message"`
}

func (p *MyPlugin) Meta() nexus.PluginMeta {
    return nexus.PluginMeta{
        Name:        "my.Plugin",
        Description: "My custom plugin",
        Version:     1,
        ArgsSchemaJSON: json.RawMessage(`{
            "type": "object",
            "properties": {
                "message": {"type": "string"}
            }
        }`),
    }
}

func (p *MyPlugin) Execute(ctx context.Context, args MyPluginArgs) (string, error) {
    // Your task logic here
    return fmt.Sprintf("Processed: %s", args.Message), nil
}
```

### 2. Initialize NexusCore

```go
// Connect to Redis
redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

// Connect to MongoDB
mongoClient, err := mongo.Connect(ctx, 
    options.Client().ApplyURI("mongodb://localhost:27017"))

// Register plugins
plugins := map[string]nexus.Plugin{
    "my.Plugin": &MyPlugin{},
}

// Create Redis cache for task queue
redisCache, err := cache.NewRedis(ctx, cache.RedisConfig{
    Addr:   "localhost:6379",
    Logger: logger,
})

// Create task queue
taskQueue := nexus.NewRedisTaskQueue(redisCache, "my_queue")

// Initialize NexusCore
core, err := nexus.NewNexusCore(ctx, nexus.NexusCoreBackendArgs{
    Redis: nexus.RedisArgs{
        Url: "localhost:6379",
        Db:  0,
    },
    MongoDbClient:          mongoClient,
    Plugins:                plugins,
    Logger:                 logger,
    MaxPluginWorkers:       8,
    TaskStateQueue:         taskQueue,
    MaxFlowQueueLength:     1000,
    ScanAndFixFlowInterval: 2 * time.Second,
})

// Start processing
go core.Run(8) // 8 worker goroutines
```

### 3. Submit Tasks

```go
event := nexus.EventDetail{
    DelegationType: "my.Plugin",
    Payload: `{"message": "Hello, World!"}`,
    MaxAttempts: 3,
    Attempts: 0,
}

backoffStrategy := backoff.NewExponentialBackOff()
core.SubmitEvent(event, backoffStrategy)
```

## CLI

The `nexus` CLI lets you submit events and check queue health.

```bash
go run ./cmd/nexus --help

# Submit an event
go run ./cmd/nexus submit \
    --delegation example.TextGenerator \
    --payload '{"filename":"sample.txt","words":120,"lineWidth":12}'

# Check health and queue depth
go run ./cmd/nexus health
```

## Docker Compose (Dev)

Bring up Redis + MongoDB + Prometheus:

```bash
docker compose up -d
```

Run the quickstart in a container:

```bash
docker compose --profile demo up
```

## Built-in Plugins

The demo includes several production-ready plugins:

### Image Generator
Generates geometric pattern images with customizable dimensions, patterns, and color schemes.

```json
{
  "width": 800,
  "height": 600,
  "pattern": "gradient",
  "colorScheme": "blue",
  "filename": "output.png"
}
```

**Patterns**: gradient, circles, squares, stripes  
**Color Schemes**: blue, red, green, rainbow, random

### Pattern Drawer
Creates complex geometric patterns and designs.

```json
{
  "width": 800,
  "height": 800,
  "patterns": ["spiral", "mandala"],
  "colors": ["#FF5733", "#33FF57"],
  "complexity": 7
}
```

**Patterns**: checkerboard, spiral, waves, mandala, hexagons, fractals

### MapReduce Processor
Performs parallel data processing operations.

```json
{
  "inputFile": "data.json",
  "operation": "sum",
  "outputFile": "result.json",
  "workers": 4
}
```

**Operations**: wordcount, sum, avg, groupby

### Random Image Retriever
Selects and copies random images from a directory.

```json
{
  "sourceDir": "./images",
  "count": 5,
  "copyTo": "./selected",
  "extensions": [".png", ".jpg"]
}
```

### File Zipper
Creates compressed archives from files and directories.

```json
{
  "sourcePaths": ["dir1", "dir2"],
  "outputFile": "archive.zip",
  "compression": 6
}
```

## Pipeline Demo

The included demo showcases a complete multi-stage pipeline:

```bash
go run main.go
```

This demonstrates:
1. **Image Generation** - Creates 10 images with various patterns
2. **Pattern Drawing** - Generates complex geometric designs
3. **Data Processing** - Performs MapReduce operations
4. **Image Selection** - Randomly retrieves images
5. **Archiving** - Compresses all outputs into zip files

## Configuration

### NexusCore Options

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `MaxPluginWorkers` | int | Number of concurrent workers | 8 |
| `MaxFlowQueueLength` | int | Maximum queue size | 1000 |
| `ScanAndFixFlowInterval` | duration | Interval for checking stalled tasks | 2s |
| `StreamCapacity` | int | Event stream buffer size | 1000 |

### Task Options

| Parameter | Type | Description |
|-----------|------|-------------|
| `DelegationType` | string | Plugin name to execute |
| `Payload` | string | JSON-encoded task arguments |
| `MaxAttempts` | int | Maximum retry attempts |
| `Attempts` | int | Current attempt count |

## Error Handling

Tasks automatically retry on failure with exponential backoff:

```go
backoffStrategy := backoff.NewExponentialBackOff()
backoffStrategy.MaxElapsedTime = 30 * time.Second
backoffStrategy.InitialInterval = 1 * time.Second
backoffStrategy.Multiplier = 2.0
```

## Monitoring

Task states are tracked through Redis keys:
- `task_{id}`: Individual task state
- `{queue_name}`: Pending tasks

Check queue status:
```go
queueLen, _ := redisClient.LLen(ctx, queueName).Result()
taskKeys, _ := redisClient.Keys(ctx, "task_*").Result()
```

## Best Practices

1. **Plugin Design**: Keep plugins focused on single responsibilities
2. **Error Handling**: Always return descriptive errors from plugins
3. **Resource Management**: Use `defer` for cleanup in plugins
4. **Concurrency**: Design plugins to be thread-safe
5. **Payload Size**: Keep task payloads small; use references to large data
6. **Timeouts**: Set appropriate context timeouts for long-running tasks

## Testing

```bash
# Run tests
go test ./...

# Run with race detector
go test -race ./...

# Run benchmarks
go test -bench=. ./...
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Plugin Development Guidelines

When creating new plugins:
- Implement the `Plugin` interface completely
- Provide comprehensive JSON schemas for validation
- Include clear error messages
- Add unit tests for your plugin
- Document plugin behavior and parameters

## License

Under MIT License

## Acknowledgments

- Built with [go-redis](https://github.com/redis/go-redis)
- Uses [MongoDB Go Driver](https://github.com/mongodb/mongo-go-driver)
- Logging powered by [zerolog](https://github.com/rs/zerolog)
- Backoff strategies from [cenkalti/backoff](https://github.com/cenkalti/backoff)

## Support

- 📧 Email: [caleberioluwa@gmail.com]
- 🐛 Issues: [GitHub Issues](https://github.com/caleberi/grain/issues)
- 📖 Documentation: [Wiki](https://github.com/caleberi/grain/wiki)

## Roadmap

- [ ] Web UI for task monitoring
- [ ] Prometheus metrics integration
- [ ] Distributed tracing support
- [ ] Task scheduling and cron support
- [ ] Plugin marketplace
