// Package cache provides a Redis-backed caching implementation for storing and retrieving
// arbitrary data in Go applications.
//
// The RedisCache struct wraps a Redis client and offers convenient methods for interacting
// with a Redis server as a cache. It supports storing, retrieving, and deleting key-value pairs,
// as well as setting expiration times for cached entries.
//
// # Usage
//
// To use RedisCache, first create a Redis client using the go-redis package, then
// initialize a RedisCache instance with it:
//
//	import (
//	    "github.com/redis/go-redis/v9"
//	    "yourmodule/cache"
//	)
//
//	// Create a Redis client
//	client := redis.NewClient(&redis.Options{
//	    Addr: "localhost:6379",
//	})
//
//	// Initialize the cache
//	c := cache.NewRedisCache(client)
//
//	// Set a value with expiration
//	err := c.Set(ctx, "key", "value", time.Minute)
//	if err != nil {
//	    // handle error
//	}
//
//	// Get a value
//	val, err := c.Get(ctx, "key")
//	if err == redis.Nil {
//	    // key does not exist
//	} else if err != nil {
//	    // handle error
//	} else {
//	    // use val
//	}
//
//	// Delete a value
//	err = c.Delete(ctx, "key")
//
// The RedisCache struct is safe for concurrent use by multiple goroutines.
//
// # Example
//
//	cache := cache.NewRedisCache(redisClient)
//	err := cache.Set(ctx, "session:123", sessionData, 10*time.Minute)
//	data, err := cache.Get(ctx, "session:123")
//
// See the RedisCache struct and its methods for more details.
package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

var compressedResultPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 4096)
	},
}

var gzipWriterPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
		return w
	},
}

// Value represents the data stored in the cache along with metadata
type Value struct {
	ExpireIn   time.Duration // Duration until expiration
	StoredTime time.Time     // When the value was stored
	SyncTime   time.Time     // Last sync time
	Data       any           // The actual cached data

	// For data optimization for large data
	DataSize   int    // Size of original data in bytes
	CheckSum   string // SHA-256 checksum of original data
	Compressed []byte // Compressed version of data
}

// syncHistoryStat stores statistics about cache sync operations
type syncHistoryStat struct {
	expired  int           // Number of expired items
	duration time.Duration // How long the sync took
	total    int           // Total items checked
}

// RedisCache provides a two-level caching system with Redis as the primary storage
// and an in-memory cache for faster access
type RedisCache struct {
	InternalCache            *redis.Client
	logger                   zerolog.Logger
	resourceCache            map[string]Value
	resourceMutex            sync.RWMutex
	syncDuration             time.Duration
	percentageInMemoryTime   float64
	compressionThresholdInMB int

	syncHistorySampleSize int
	syncHistoryTrendMutex sync.Mutex
	syncHistoryTrend      []syncHistoryStat
	syncHistoryIndex      int
	syncHistoryCount      int

	expiryEWMA           float64
	expiryEWMAThresholds map[string]time.Duration
	alpha                float64

	maxCacheEntries int
	memoryCapMB     int
	trimRatio       float64

	skipCompression bool

	isShutdown sync.Once
	ctx        context.Context
	cancel     context.CancelFunc
}

// RedisConfig contains configuration for the Redis cache
type RedisConfig struct {
	Addr                     string
	Username                 string
	Password                 string
	Db                       int
	MemoryCapMB              int
	MaxCacheEntries          int
	PercentageInMemoryTime   float64
	TrimRatio                float64
	SyncDuration             time.Duration
	Logger                   zerolog.Logger
	Alpha                    float64
	SyncHistorySampleSize    int
	CompressionThresholdInMB int
	SkipCompression          bool
}

// CacheAccessor defines the interface for basic cache operations
type CacheAccessor interface {
	RetrieveFromCache(key string, timeout time.Duration) (any, error)
	SaveInCache(key string, value any, expireIn, timeout time.Duration) error
	Shutdown()
}

// NewRedis creates a new Redis client and returns a RedisCache instance
func NewRedis(ctx context.Context, config RedisConfig) (*RedisCache, error) {
	logger := config.Logger
	logger.Info().Msgf("address: %s | username: %s | password: %s", config.Addr, config.Username, "******")
	logger.Info().Msg("Initializing Redis client...")

	rd := redis.NewClient(&redis.Options{
		Username: config.Username,
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.Db,
	})

	logger.Info().Msgf("Redis client created for address: %s", config.Addr)

	rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	logger.Info().Msg("Pinging Redis to verify connection...")
	if err := rd.Ping(rctx).Err(); err != nil {
		logger.Error().Err(err).Msg("Failed to connect to Redis")
		return nil, err
	}

	logger.Info().Msg("Redis connection successful.")
	cacheCtx, cacheCancel := context.WithCancel(ctx)

	cache := &RedisCache{
		InternalCache:          rd,
		logger:                 logger,
		resourceCache:          make(map[string]Value),
		ctx:                    cacheCtx,
		cancel:                 cacheCancel,
		percentageInMemoryTime: config.PercentageInMemoryTime,
		alpha:                  config.Alpha,
		expiryEWMAThresholds: map[string]time.Duration{
			"high":   5 * time.Minute,
			"medium": 15 * time.Minute,
			"low":    25 * time.Minute,
		},
		maxCacheEntries:          config.MaxCacheEntries,
		memoryCapMB:              config.MemoryCapMB,
		trimRatio:                config.TrimRatio,
		syncDuration:             config.SyncDuration,
		syncHistorySampleSize:    config.SyncHistorySampleSize,
		syncHistoryTrend:         make([]syncHistoryStat, config.SyncHistorySampleSize),
		syncHistoryIndex:         0,
		syncHistoryCount:         0,
		compressionThresholdInMB: config.CompressionThresholdInMB,
		skipCompression:          config.SkipCompression,
	}

	// Start background sync goroutine
	go cache.syncWorker()

	return cache, nil
}

// syncWorker handles periodic cache sync operations
func (rd *RedisCache) syncWorker() {
	for {
		select {
		case <-rd.ctx.Done():
			rd.logger.Debug().Msg("Sync worker shutting down")
			return
		default:
			rd.doSync()

			rd.syncHistoryTrendMutex.Lock()
			ewma := rd.expiryEWMA
			rd.syncHistoryTrendMutex.Unlock()

			var sleep time.Duration
			switch {
			case ewma > 100:
				sleep = rd.expiryEWMAThresholds["high"]
			case ewma > 20:
				sleep = rd.expiryEWMAThresholds["medium"]
			default:
				sleep = rd.expiryEWMAThresholds["low"]
			}

			rd.logger.Debug().
				Float64("ewma", ewma).
				Dur("next_sleep", sleep).
				Msg("Next cache sync scheduled")

			select {
			case <-time.After(sleep):
				continue
			case <-rd.ctx.Done():
				rd.logger.Debug().Msg("Sync worker shutting down")
				return
			}
		}
	}
}

// doCheckMemory checks memory usage and trims the cache if necessary
func (rd *RedisCache) doCheckMemory() {
	rd.resourceMutex.RLock()
	resourceMemoryUsageEstimateInMB := uint64(DeepSizeOf(rd.resourceCache) / 1024 / 1024)
	rd.resourceMutex.RUnlock()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	rd.logger.Debug().
		Uint64("resourceMemoryUsageEstimateInMB", resourceMemoryUsageEstimateInMB).
		Int("cap_MB", rd.memoryCapMB).
		Msg("Memory check")

	if resourceMemoryUsageEstimateInMB > uint64(rd.memoryCapMB) {
		rd.logger.Warn().
			Uint64("resourceMemoryUsageEstimateInMB", resourceMemoryUsageEstimateInMB).
			Int("cap_MB", rd.memoryCapMB).
			Float64("trim_ratio", rd.trimRatio).
			Msg("Memory cap exceeded, trimming in-memory cache")

		rd.trimCacheBySyncTime(rd.trimRatio)
	}
}

func (rd *RedisCache) trimCacheBySyncTime(ratio float64) {
	rd.resourceMutex.Lock()
	defer rd.resourceMutex.Unlock()

	if len(rd.resourceCache) == 0 {
		return
	}

	type entry struct {
		key string
		t   time.Time
	}
	var entries []entry
	for k, v := range rd.resourceCache {
		entries = append(entries, entry{key: k, t: v.SyncTime})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].t.Before(entries[j].t)
	})

	trimCount := int(float64(len(entries)) * ratio)
	if trimCount == 0 && len(entries) > 0 {
		trimCount = 1 // Ensure we trim at least one entry
	}

	if trimCount > 0 {
		keysEvicted := make([]string, 0, trimCount)
		for i := range trimCount {
			key := entries[i].key
			delete(rd.resourceCache, key)
			keysEvicted = append(keysEvicted, key)
		}
		rd.logger.Debug().Strs("keys", keysEvicted).
			Int("count", trimCount).
			Msg("Evicted entries due to memory pressure")
	}
}

// doSync synchronizes the in-memory cache, removing expired entries
func (rd *RedisCache) doSync() {
	rd.logger.Info().Msg("Running cache sync...")
	start := time.Now()
	expiredCount := 0
	totalCount := 0

	rd.resourceMutex.Lock()
	for key, info := range rd.resourceCache {
		totalCount++
		if info.ExpireIn == 0 || info.StoredTime.Add(info.ExpireIn).After(time.Now()) {
			continue
		}

		delete(rd.resourceCache, key)
		expiredCount++
		rd.logger.Debug().Str("key", key).Msg("In-memory cache expired")
	}
	rd.resourceMutex.Unlock()

	duration := time.Since(start)
	rd.syncHistoryTrendMutex.Lock()
	rd.syncHistoryTrend[rd.syncHistoryIndex] = syncHistoryStat{
		expired:  expiredCount,
		duration: duration,
		total:    totalCount,
	}

	rd.syncHistoryIndex = (rd.syncHistoryIndex + 1) % rd.syncHistorySampleSize

	if rd.syncHistoryCount < rd.syncHistorySampleSize {
		rd.syncHistoryCount++
	}

	rd.syncHistoryTrendMutex.Unlock()

	rd.updateEWMAFromSamples()
	rd.logger.Info().
		Int("expired", expiredCount).
		Int("total", totalCount).
		Float64("ewma", rd.expiryEWMA).
		Dur("duration", duration).
		Msg("Cache sync complete")
	rd.doCheckMemory()
}

// updateEWMAFromSamples updates the exponentially weighted moving average
// of cache expiry rates based on sync history
func (rd *RedisCache) updateEWMAFromSamples() {
	rd.syncHistoryTrendMutex.Lock()
	defer rd.syncHistoryTrendMutex.Unlock()

	if rd.syncHistoryCount == 0 {
		return
	}

	// Calculate the index of the newest sample (the one we just added)
	// It's the (index - 1) with wrapping
	newestIdx := (rd.syncHistoryIndex + rd.syncHistorySampleSize - 1) % rd.syncHistorySampleSize
	newest := rd.syncHistoryTrend[newestIdx]

	if newest.total == 0 || newest.duration == 0 {
		return
	}

	newestRate := (float64(newest.expired) / float64(newest.total)) /
		(float64(newest.duration) / float64(time.Second))

	if rd.syncHistoryCount == 1 {
		rd.expiryEWMA = newestRate
	} else {
		// Otherwise apply the EWMA formula: ewma(t) = α * x(t) + (1-α) * ewma(t-1)
		rd.expiryEWMA = rd.alpha*newestRate + (1-rd.alpha)*rd.expiryEWMA
	}

	rd.logger.Debug().
		Float64("current_ewma", rd.expiryEWMA).
		Int("sample_count", len(rd.syncHistoryTrend)).
		Msg("Updated expiry EWMA")
}

func (rd *RedisCache) DeleteFromCache(key string, timeout time.Duration) error {
	rd.resourceMutex.Lock()
	_, ok := rd.resourceCache[key]
	if ok {
		delete(rd.resourceCache, key)
	}
	rd.resourceMutex.Unlock()

	rdctx, cancel := context.WithTimeout(rd.ctx, timeout)
	defer cancel()
	return rd.InternalCache.Del(rdctx, key).Err()
}

// RetrieveFromCache gets a value from cache, trying in-memory first then Redis
func (rd *RedisCache) RetrieveFromCache(key string, timeout time.Duration) (string, error) {

	rd.resourceMutex.RLock()
	cached, ok := rd.resourceCache[key]
	inMemExpired := cached.ExpireIn != 0 && time.Now().After(cached.StoredTime.Add(cached.ExpireIn))
	rd.resourceMutex.RUnlock()

	if ok && !inMemExpired {
		rd.logger.Debug().Str("key", key).Msg("Checking in-memory cached value")
		// Update SyncTime to keep this item "fresh" for cache eviction
		rd.resourceMutex.Lock()
		if val, exists := rd.resourceCache[key]; exists {
			val.SyncTime = time.Now()
			rd.resourceCache[key] = val
		}
		rd.resourceMutex.Unlock()

		if cached.Compressed != nil {
			decompressed, err := decompressData(cached.Compressed, cached.CheckSum)
			if err != nil {
				rd.logger.Warn().Err(err).Msg("Decompression failed, fetching from Redis")
				ctx, cancel := context.WithTimeout(rd.ctx, timeout)
				defer cancel()
				data, err := rd.getFromRedis(ctx, key)
				if err != nil {
					rd.logger.Warn().Err(err).Msg("Fetching from redis failed")
					return "", err
				}
				return data, nil
			} else {
				rd.logger.Info().Str("key", key).Msg("Cache hit (in-memory, decompressed)")
				return string(decompressed), nil
			}
		} else {
			rd.logger.Info().Str("key", key).Msg("Cache hit (in-memory)")
			if v, ok := cached.Data.(string); ok {
				return v, nil
			}
			b, err := json.Marshal(cached.Data)
			if err != nil {
				rd.logger.Error().Str("key", key).
					Msg("A problem occurred when converting in-memory data to string")
			}
			return string(b), nil
		}
	}

	rd.logger.Debug().Str("key", key).Msg("Fetching from Redis")
	ctx, cancel := context.WithTimeout(rd.ctx, timeout)
	defer cancel()
	data, err := rd.getFromRedis(ctx, key)
	if err != nil {
		if err == redis.Nil {
			rd.logger.Debug().Str("key", key).Msg("Key not found in Redis")
			return "", nil // Key not found, return nil value without error
		}
		rd.logger.Warn().Err(err).Str("key", key).Msg("Error fetching from Redis")
		return "", err
	}
	return data, nil
}

func (rd *RedisCache) getFromRedis(ctx context.Context, key string) (string, error) {
	data, err := rd.InternalCache.Get(ctx, key).Result()
	if err == redis.Nil {
		rd.logger.Warn().
			Str("key", key).
			Msg("Key not found in Redis (may have expired)")

		return "", redis.Nil
	} else if err != nil {
		rd.logger.Error().Err(err).Str("key", key).Msg("Redis GET error")
		return "", err
	}

	rd.logger.Info().Str("key", key).Msg("Cache hit (Redis)")
	return data, nil
}

// SaveInCache stores a value in both Redis and the in-memory cache
func (rd *RedisCache) SaveInCache(key string, value any, expireIn, timeout time.Duration) error {

	info := Value{
		SyncTime:   time.Now(),
		StoredTime: time.Now(),
		Data:       value,
	}

	dataSize, _, err := calculateDataSize(value)

	if err != nil {
		return err
	}

	if dataSize > rd.compressionThresholdInMB && !rd.skipCompression {
		compressedData, err := compressData(value)
		if err != nil {
			rd.logger.Warn().Err(err).Msg("Compression failed, storing original")
			info.Data = value
		} else {
			info.Compressed = compressedData
			info.DataSize = dataSize
			info.CheckSum, err = generateChecksum(value)
			if err != nil {
				rd.logger.Warn().Err(err).Msg("Checksum  generation failed, storing original")
				info.Data = value
			} else {
				info.Data = nil
			}
		}
	} else {
		info.Data = value
	}

	if expireIn != 0 {
		info.ExpireIn = time.Duration((rd.percentageInMemoryTime * float64(expireIn)) / 100)
	}

	if len(rd.resourceCache) >= rd.maxCacheEntries {
		rd.evictOldestEntry()
	}
	rd.resourceMutex.Lock()
	rd.resourceCache[key] = info
	rd.resourceMutex.Unlock()

	ctx, cancel := context.WithTimeout(rd.ctx, timeout)
	defer cancel()

	storedValue, err := prepareStoredValue(value)
	if err != nil {
		return err
	}

	err = rd.InternalCache.Set(ctx, key, storedValue, expireIn).Err()
	if err != nil {
		rd.logger.Error().Err(err).Str("key", key).Msg("Failed to set value in Redis")
		return err
	}
	rd.logger.Info().Str("key", key).Msg("Value successfully saved in Redis and memory")
	return nil
}

func (rd *RedisCache) evictOldestEntry() {
	var oldestKey string
	var oldestTime time.Time

	rd.resourceMutex.RLock()
	for k, v := range rd.resourceCache {
		if oldestKey == "" || v.SyncTime.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.SyncTime
		}
	}
	rd.resourceMutex.RUnlock()

	if oldestKey != "" {
		rd.resourceMutex.Lock()
		delete(rd.resourceCache, oldestKey)
		rd.resourceMutex.Unlock()
		rd.logger.Warn().Str("evicted_key", oldestKey).
			Msg("Evicted oldest entry due to max cache size")
	}
}

func (rd *RedisCache) Shutdown() {
	rd.isShutdown.Do(func() {
		rd.logger.Info().Msg("Shutting down cache and canceling context")
		rd.cancel()
	})
}

func compressData(data any) ([]byte, error) {
	var rawData []byte

	switch v := data.(type) {
	case string:
		rawData = []byte(v)
	case []byte:
		rawData = v
	default:
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		rawData = jsonData
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	gz := gzipWriterPool.Get().(*gzip.Writer)
	gz.Reset(buf)
	defer func() {
		gz.Close()
		gzipWriterPool.Put(gz)
	}()
	if _, err := gz.Write(rawData); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	result := compressedResultPool.Get().([]byte)
	compressedLen := buf.Len()
	if cap(result) < compressedLen {
		result = make([]byte, compressedLen)
	} else {
		result = result[:compressedLen]
	}
	copy(result, buf.Bytes())

	return result, nil
}

func generateChecksum(data any) (string, error) {
	var (
		rawData []byte
		err     error
	)

	switch v := data.(type) {
	case string:
		rawData = []byte(v)
	case []byte:
		rawData = v
	default:
		rawData, err = json.Marshal(data)
		if err != nil {
			return "", fmt.Errorf("failed to marshal data for checksum: %w", err)
		}
	}

	hash := sha256.Sum256(rawData)
	checksum := hex.EncodeToString(hash[:])

	return checksum, nil
}

func decompressData(compressedData []byte, expectedChecksum string) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	hash := sha256.Sum256(decompressed)
	actualChecksum := hex.EncodeToString(hash[:])

	if expectedChecksum != "" && actualChecksum != expectedChecksum {
		return nil, fmt.Errorf("checksum mismatch: expected %s, got %s",
			expectedChecksum, actualChecksum)
	}

	// if len(decompressed) > 0 && (decompressed[0] == '{' || decompressed[0] == '[') {
	// 	var result any
	// 	if err := json.Unmarshal(decompressed, &result); err == nil {
	// 		return result, nil
	// 	}
	// 	return "", nil
	// }
	return decompressed, nil
}

func calculateDataSize(value any) (int, []byte, error) {
	switch val := value.(type) {
	case string:
		return len(val), nil, nil

	case int:
		return len(strconv.FormatInt(int64(val), 10)), nil, nil
	case int8:
		return len(strconv.FormatInt(int64(val), 10)), nil, nil
	case int16:
		return len(strconv.FormatInt(int64(val), 10)), nil, nil
	case int32:
		return len(strconv.FormatInt(int64(val), 10)), nil, nil
	case int64:
		return len(strconv.FormatInt(val, 10)), nil, nil

	case uint:
		return len(strconv.FormatUint(uint64(val), 10)), nil, nil
	case uint8:
		return len(strconv.FormatUint(uint64(val), 10)), nil, nil
	case uint16:
		return len(strconv.FormatUint(uint64(val), 10)), nil, nil
	case uint32:
		return len(strconv.FormatUint(uint64(val), 10)), nil, nil
	case uint64:
		return len(strconv.FormatUint(val, 10)), nil, nil

	case float32:
		return len(strconv.FormatFloat(float64(val), 'f', -1, 64)), nil, nil
	case float64:
		return len(strconv.FormatFloat(val, 'f', -1, 64)), nil, nil

	case bool:
		return 5, nil, nil // "false" is the longest possible string

	case []byte:
		return len(val), nil, nil

	default:
		jsonBuf, err := json.Marshal(val)
		if err != nil {
			return 0, nil, fmt.Errorf("error marshaling value: %w", err)
		}
		return len(jsonBuf), jsonBuf, nil
	}
}

func prepareStoredValue(value any) (any, error) {
	switch val := value.(type) {
	case string, []byte,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		bool:
		return val, nil
	default:
		jsonData, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal value for Redis: %w", err)
		}
		return string(jsonData), nil
	}
}
