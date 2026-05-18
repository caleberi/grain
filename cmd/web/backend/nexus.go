package server

import (
	"context"
	"fmt"
	"grain/nexus"
	"io/fs"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	Port      string
	RedisAddr string
	MongoURI  string
}

type NexusWebServer struct {
	logger zerolog.Logger
	fiber  *fiber.App
	redis  *redis.Client
	mongo  *mongo.Client
	nexus  *nexus.NexusCore

	taskStateStreamer *TaskStateStreamer
	config            Config
}

func (a *NexusWebServer) Shutdown() error {
	if a.nexus != nil {
		a.nexus.Shutdown()
	}
	if a.mongo != nil {
		err := a.mongo.Disconnect(context.Background())
		if err != nil {
			return err
		}
	}
	if a.redis != nil {
		err := a.redis.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

var nexusCore *nexus.NexusCore

func NewNexusWebServer(cfg Config, logger zerolog.Logger) (*NexusWebServer, error) {

	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	mongoClient, err := mongo.Connect(
		context.Background(),
		options.Client().ApplyURI(cfg.MongoURI),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	queue := nexus.NewRedisTaskQueue(
		&nexus.RedisCache{InternalCache: redisClient},
		"task_state_queue",
	)

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	core, err := nexus.NewNexusCore(
		context.Background(),
		nexus.NexusCoreBackendArgs{
			Redis:                           nexus.RedisArgs{Url: cfg.RedisAddr, Db: 0},
			MongoDbClient:                   mongoClient,
			Logger:                          logger,
			TaskStateQueue:                  queue,
			MaxFlowQueueLength:              1000,
			ScanAndFixFlowInterval:          2 * time.Second,
			StreamCapacity:                  1000,
			MaxConcurrentPluginPerExecution: 1,
		})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to core: %w", err)
	}

	fiberApp := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			return c.Status(code).JSON(fiber.Map{"error": err.Error()})
		},
	})
	fiberApp.Use(cors.New())
	fiberApp.Use(recover.New())

	app := &NexusWebServer{
		fiber:  fiberApp,
		config: cfg,
		redis:  redisClient,
		mongo:  mongoClient,
		nexus:  core,
		logger: logger,
	}

	nexusCore = core

	app.taskStateStreamer = NewTaskStateStreamer()
	go app.taskStateStreamer.Start(context.Background(), logger, queue)

	app.setupRoutes()
	return app, nil
}

func (a *NexusWebServer) setupRoutes() {
	a.fiber.Static("/", "./cmd/web/frontend/dist")

	api := a.fiber.Group("/api")
	api.Post("/plugins/create", a.CreatePlugin)

	api.Post("/plugins/compile", a.CompilePlugin)
	api.Get("/plugins", a.ListPlugins)
	api.Post("/events/submit", a.SubmitEvent)
	api.Get("/events", a.ListEvents)
	api.Get("/events/:id", a.GetEvent)
	api.Get("/health", a.Health)
	api.Get("/task-stream", a.StreamTaskStates)
}

func (a *NexusWebServer) Start() error {
	a.logger.Info().Msgf("Starting Nexus Web Server on :%s", a.config.Port)

	if err := a.loadCompiledPluginsIntoCore(); err != nil {
		a.logger.Warn().Err(err).Msg("Failed to fully load compiled plugins into core")
	}

	go a.nexus.Run(2)

	return a.fiber.Listen(":" + a.config.Port)
}

func (a *NexusWebServer) CreatePlugin(c *fiber.Ctx) error {
	var request CreatePluginRequest
	if err := c.BodyParser(&request); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request"})
	}

	if request.Name == "" || request.Code == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Name and code are required"})
	}

	err := storePluginCode(request.Name, request.Code, request.Description)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	soPath, err := compilePluginCode(request.Name, request.Code)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	if err := a.loadPluginIntoCore(soPath); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"message": "Plugin created and compiled successfully",
		"name":    request.Name,
		"path":    soPath,
	})
}

func (a *NexusWebServer) CompilePlugin(c *fiber.Ctx) error {
	var request GetPluginReueust
	if err := c.BodyParser(&request); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request"})
	}

	if request.Name == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Plugin name is required"})
	}

	code, err := getPluginCode(request.Name)
	if err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "Plugin not found"})
	}

	soPath, err := compilePluginCode(request.Name, code)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	a.logger.Info().Str("plugin", request.Name).Str("path", soPath).Msg("plugin compiled and loaded into core")

	return c.JSON(fiber.Map{
		"message": "Plugin compiled successfully",
		"name":    request.Name,
		"path":    soPath,
	})
}

func (a *NexusWebServer) ListPlugins(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"plugins": listStoredPlugins()})
}

func (a *NexusWebServer) SubmitEvent(c *fiber.Ctx) error {
	var request SubmitEventRequest
	if err := c.BodyParser(&request); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid request"})
	}

	if request.DelegationType == "" || request.Payload == "" {
		return c.Status(400).JSON(fiber.Map{
			"error": "DelegationType and Payload are required",
		})
	}

	if request.MaxAttempts <= 0 {
		request.MaxAttempts = 3
	}

	eventID, err := submitEventToCore(a.nexus,
		request.DelegationType, request.Payload,
		request.MaxAttempts, uint8(request.Priority),
	)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"message": "Event submitted successfully",
		"eventID": eventID,
	})
}

func (a *NexusWebServer) ListEvents(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	limit, _ := strconv.Atoi(c.Query("limit", "20"))
	state := c.Query("state")
	delegationType := c.Query("delegationType")

	events, total, err := getEventList(a.nexus, page, limit, state, delegationType)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"events": events,
		"total":  total,
		"page":   page,
		"limit":  limit,
	})
}

func (a *NexusWebServer) GetEvent(c *fiber.Ctx) error {
	eventID := c.Params("id")
	if eventID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "Event ID is required"})
	}

	event, err := getEventByID(a.nexus, eventID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"event": event})
}

func (a *NexusWebServer) Health(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	redisOK := a.redis.Ping(ctx).Err() == nil
	mongoOK := a.mongo.Ping(ctx, nil) == nil

	status := "healthy"
	if !redisOK || !mongoOK {
		status = "unhealthy"
	}

	return c.JSON(fiber.Map{
		"status": status,
		"redis":  redisOK,
		"mongo":  mongoOK,
	})
}

func (a *NexusWebServer) StreamTaskStates(c *fiber.Ctx) error {
	return a.taskStateStreamer.StreamFiber(c)
}

func (a *NexusWebServer) loadCompiledPluginsIntoCore() error {
	var firstErr error
	walkErr := filepath.WalkDir(
		pluginsDir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return nil
			}

			if d.IsDir() {
				return nil
			}

			if filepath.Ext(path) != ".so" {
				return nil
			}

			if err := a.loadPluginIntoCore(path); err != nil {
				a.logger.Warn().Err(err).Msgf("Failed loading plugin %s", path)
				if firstErr == nil {
					firstErr = err
				}
				return nil
			}

			a.logger.Info().Msgf("Loaded plugin into core: %s", path)
			return nil
		})

	if walkErr != nil {
		return walkErr
	}

	return firstErr
}

func (a *NexusWebServer) loadPluginIntoCore(soPath string) error {
	loadedPlugin, err := nexus.LoadPlugin(soPath)
	if err != nil {
		return fmt.Errorf("failed to load plugin: %w", err)
	}

	meta := loadedPlugin.Meta()
	if meta.Name == "" {
		return fmt.Errorf("plugin returned empty name")
	}

	if err := a.nexus.RegisterPlugin(meta.Name, loadedPlugin); err != nil {
		return fmt.Errorf("failed to register plugin %s: %w", meta.Name, err)
	}

	a.logger.Info().
		Str("plugin", meta.Name).
		Str("path", soPath).
		Int("version", meta.Version).
		Msg("plugin loaded into core")

	return nil
}
