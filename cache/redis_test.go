package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var keyBufPool = sync.Pool{
	New: func() any { return new(strings.Builder) },
}

func makeKey(base string, i int) string {
	sb := keyBufPool.Get().(*strings.Builder)
	sb.Reset()
	defer keyBufPool.Put(sb)

	sb.WriteString(base)
	sb.WriteString(strconv.Itoa(i))
	return sb.String()
}

func setRedisCache(t *testing.T, ctx context.Context) (*RedisCache, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	require.NotNil(t, mr, "Could not initialize test redis instance")
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	rctx, cancel := context.WithCancel(ctx)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	cache := &RedisCache{
		InternalCache:          client,
		logger:                 logger,
		resourceCache:          make(map[string]Value),
		ctx:                    rctx,
		cancel:                 cancel,
		percentageInMemoryTime: 50,
		alpha:                  0.5,
		expiryEWMAThresholds: map[string]time.Duration{
			"high":   5 * time.Second,
			"medium": 10 * time.Second,
			"low":    15 * time.Second,
		},
		maxCacheEntries:          100,
		memoryCapMB:              100,
		trimRatio:                0.25,
		syncDuration:             time.Minute,
		syncHistorySampleSize:    5,
		syncHistoryTrend:         make([]syncHistoryStat, 1024),
		compressionThresholdInMB: 10 * 1024,
	}

	return cache, mr
}

func TestCacheSaveAndRetrieve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	testcases := []struct {
		name        string
		key         string
		value       any
		expireIn    time.Duration
		forceRedis  bool
		expectError bool
	}{
		{
			name:     "String Value",
			key:      "test-string-key",
			value:    "test-string-value",
			expireIn: 5 * time.Second,
		},
		{
			name:     "Integer Value",
			key:      "test-int-key",
			value:    42,
			expireIn: 7 * time.Second,
		},
		{
			name: "Map Value",
			key:  "test-map-key",
			value: map[string]any{
				"name":  "test",
				"count": 42,
			},
			expireIn: time.Second,
		},
		{
			name:     "Slice Value",
			key:      "test-slice-key",
			value:    []string{"item1", "item2", "item3"},
			expireIn: time.Hour,
		},
		{
			name: "Complex Nested Value From Redis",
			key:  "test-complex-key",
			value: map[string]any{
				"name":  "test",
				"items": []any{1, 2, 3},
				"nested": map[string]any{
					"a": "b",
				},
			},
			expireIn:   time.Hour,
			forceRedis: true,
		},
		{
			name:     "Zero Expiration",
			key:      "test-zero-expiry",
			value:    "value without expiration",
			expireIn: 0,
		},
		{
			name:        "Non-Existent Key",
			key:         "non-existent-key",
			value:       nil,
			expectError: false,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != nil {
				err := cache.SaveInCache(tt.key, tt.value, tt.expireIn, 5*time.Second)
				require.NoError(t, err, "Failed to save in cache")
			}

			if tt.forceRedis {
				cache.resourceMutex.Lock()
				delete(cache.resourceCache, tt.key)
				cache.resourceMutex.Unlock()
			}

			result, err := cache.RetrieveFromCache(tt.key, 5*time.Second)

			if tt.expectError {
				if !assert.Error(t, err) {
					t.Fatalf("Expected=%v Got=%v", nil, err)
				}
			}

			require.NoError(t, err)
			if tt.value == nil {
				assert.Empty(t, result, "Should return empty string for non-existent key")
				return
			}

			switch val := tt.value.(type) {
			case string:
				assert.Equal(t, val, result)
			case int:
				parsed, err := strconv.Atoi(result)
				assert.NoError(t, err)
				assert.Equal(t, val, parsed)
			case float64:
				parsed, err := strconv.ParseFloat(result, 64)
				assert.NoError(t, err)
				assert.Equal(t, val, parsed)
			case bool:
				parsed, err := strconv.ParseBool(result)
				assert.NoError(t, err)
				assert.Equal(t, val, parsed)
			default:
				expectedJSON, _ := json.Marshal(val)
				assert.JSONEq(t, string(expectedJSON), string(result))
			}

		})
	}
}

func TestRedisCache_Expiration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	err := cache.SaveInCache("short-lived", "expired soon", 50*time.Millisecond, 5*time.Second)
	require.NoError(t, err)

	mr.FastForward(100 * time.Millisecond)

	_, err = cache.RetrieveFromCache("short-lived", 5*time.Second)
	require.NoError(t, err, "Should retrieve from Redis after memory expiry")

	cache.trimCacheBySyncTime(cache.trimRatio)

	cache.resourceMutex.RLock()
	_, exists := cache.resourceCache["short-lived"]
	cache.resourceMutex.RUnlock()

	assert.False(t, exists, "Key should have been removed from memory cache after expiration")
}

func TestRedisCache_Compression(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	largeValue := ""
	for i := range 20000 {
		largeValue += makeKey("data-%d", i)
	}

	err := cache.SaveInCache("large-key", largeValue, time.Hour, 5*time.Second)
	require.NoError(t, err)

	cache.resourceMutex.RLock()
	value, exists := cache.resourceCache["large-key"]
	cache.resourceMutex.RUnlock()

	require.True(t, exists)
	cache.logger.Debug().Msgf("Checksum should be calculated (sum=%s)", value.CheckSum)
	assert.NotNil(t, value.Compressed, "Large value should be compressed")
	assert.NotEmptyf(t, value.CheckSum, "Checksum should be calculated")
	assert.Nil(t, value.Data, "Original data should be nil when compressed")

	result, err := cache.RetrieveFromCache("large-key", 5*time.Second)
	require.NoError(t, err)
	assert.Equal(t, largeValue, result)

}

func TestRedisCache_MaxEntries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	cache.maxCacheEntries = 5

	for i := range 10 {
		key := makeKey("key-%d", i)
		err := cache.SaveInCache(key, fmt.Sprintf("value-%d", i), time.Hour, 5*time.Second)
		require.NoError(t, err)
	}

	cache.resourceMutex.RLock()
	count := len(cache.resourceCache)
	cache.resourceMutex.RUnlock()

	assert.LessOrEqual(t, count, cache.maxCacheEntries,
		"Cache should not exceed max entries")
}

func TestRedisCache_MemoryTrimming(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	cache.memoryCapMB = 1

	for i := range 1000 {
		key := makeKey("trim-key-%d", i)

		err := cache.SaveInCache(key, makeKey("value-%d", i), time.Hour, 5*time.Second)
		require.NoError(t, err)

		cache.resourceMutex.Lock()
		val := cache.resourceCache[key]
		val.SyncTime = time.Now().Add(time.Duration(-i) * time.Minute)
		cache.resourceCache[key] = val
		cache.resourceMutex.Unlock()
	}

	cache.doCheckMemory()

	cache.resourceMutex.RLock()
	count := len(cache.resourceCache)
	cache.resourceMutex.RUnlock()

	assert.Less(t, count, 1000, "Some cache entries should have been trimmed")
}

func TestRedisCache_EWMA(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	cache.syncHistoryTrend = []syncHistoryStat{
		{expired: 5, total: 100, duration: time.Second},
		{expired: 10, total: 100, duration: time.Second},
		{expired: 15, total: 100, duration: time.Second},
	}
	cache.updateEWMAFromSamples()

	assert.GreaterOrEqual(t, cache.expiryEWMA, 0.0)
	assert.Less(t, cache.expiryEWMA, 0.15)
}

func TestRedisCache_ConcurrentAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	const numGoroutines = 50
	done := make(chan bool)

	for i := range numGoroutines {
		go func(id int) {
			key := makeKey("concurrent-key-%d", id%5)
			if id%2 == 0 {
				err := cache.SaveInCache(key, makeKey("value-%d", id), time.Hour, 5*time.Second)
				assert.NoError(t, err)
			} else {
				_, err := cache.RetrieveFromCache(key, 5*time.Second)
				assert.NoErrorf(t, err, "Error retrieving key from cache")
			}

			done <- true
		}(i)
	}

	for range numGoroutines {
		<-done
	}
}

func TestRedisCache_Shutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()

	cache.Shutdown()

	err := cache.ctx.Err()
	assert.Error(t, err, "Context should be canceled after shutdown")

	cache.Shutdown()
}

func TestRedisCache_Redis_Unavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, mr := setRedisCache(t, ctx)

	err := cache.SaveInCache("resilient-key", "resilient-value", time.Hour, 5*time.Second)
	require.NoError(t, err)

	mr.Close()

	result, err := cache.RetrieveFromCache("resilient-key", 5*time.Second)
	require.Nil(t, err)
	assert.Equal(t, "resilient-value", result)

}

func TestCompressDecompress(t *testing.T) {
	testData := map[string]any{
		"name":  "test-compress",
		"count": 100,
		"nested": map[string]any{
			"field1": "value1",
			"field2": 42,
		},
	}

	compressed, err := compressData(testData)
	require.NoError(t, err)
	assert.NotEmpty(t, compressed)

	checksum, err := generateChecksum(testData)
	require.NoError(t, err)
	assert.NotEmpty(t, checksum)

	decompressed, err := decompressData(compressed, checksum)
	require.NoError(t, err)

	originalJSON, _ := json.Marshal(testData)
	assert.JSONEq(t, string(originalJSON), string(decompressed))
}

func TestRedisConfig(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel)

	config := RedisConfig{
		Addr:                   mr.Addr(),
		Username:               "",
		Password:               "",
		Db:                     0,
		MemoryCapMB:            100,
		MaxCacheEntries:        1000,
		PercentageInMemoryTime: 50,
		TrimRatio:              0.25,
		SyncDuration:           time.Minute,
		SyncHistorySampleSize:  5,

		Logger: logger,
		Alpha:  0.5,
	}

	client, err := NewRedis(context.Background(), config)
	require.NoError(t, err)
	assert.NotNil(t, client)

	badConfig := config
	badConfig.Addr = "invalid:6379"
	_, err = NewRedis(context.Background(), badConfig)
	assert.Error(t, err)
}

func TestCacheEvictionAndExpiry(t *testing.T) {
	ctx := context.Background()
	cache, mr := setRedisCache(t, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	cache.maxCacheEntries = 5

	t.Run("EvictionTest", func(t *testing.T) {
		for i := range 10 {
			key := fmt.Sprintf("eviction-test-%d", i)
			err := cache.SaveInCache(key, fmt.Sprintf("value-%d", i), time.Hour, 100*time.Millisecond)
			require.NoError(t, err)
		}

		cache.resourceMutex.RLock()
		size := len(cache.resourceCache)
		cache.resourceMutex.RUnlock()

		require.LessOrEqual(t, size, cache.maxCacheEntries,
			"Cache size should be limited to maxCacheEntries")
	})

	t.Run("ExpiryTest", func(t *testing.T) {

		testKey := "short-lived"
		err := cache.SaveInCache(testKey, "will-expire-soon", 50*time.Millisecond, 100*time.Millisecond)
		require.NoError(t, err)

		exists := mr.Exists(testKey)
		require.True(t, exists, "Key should exist in Redis initially")

		mr.FastForward(100 * time.Millisecond)

		exists = mr.Exists(testKey)
		require.False(t, exists, "Key should have expired in Redis")

		cache.doSync()

		cache.resourceMutex.RLock()
		_, exists = cache.resourceCache[testKey]
		cache.resourceMutex.RUnlock()
		require.True(t, exists, "Key should be in memory")
	})
}

func BenchmarkCompressData(b *testing.B) {
	value := strings.Repeat("x", 100)

	for i := 0; i < b.N; i++ {
		_, err := compressData(value)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSaveInCacheOnly(b *testing.B) {
	ctx := context.Background()
	cache, mr := setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	key := "bench-key"
	value := strings.Repeat("x", 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := cache.SaveInCache(
			makeKey(key, i), value, 0, 100*time.Millisecond)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSaveInCache(b *testing.B) {
	ctx := context.Background()
	cache, mr := setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	tests := []struct {
		name            string
		valueSize       int
		expireIn        time.Duration
		skipCompression bool
	}{
		{"SmallValue", 100, time.Minute, true},
		{"MediumValue", 5 * 1024, time.Minute, false}, // 5KB
		{"LargeValue", 20 * 1024, time.Minute, false}, // 20KB - above compression threshold
		{"NoExpiry", 100, 0, false},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			value := generateRandomString(tc.valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := "benchmark-save-" + strconv.Itoa(i)
				if tc.skipCompression {
					cache.skipCompression = true
				}
				err := cache.SaveInCache(key, value, tc.expireIn, 100*time.Millisecond)
				if err != nil {
					b.Fatalf("Error saving to cache: %v", err)
				}
			}
		})
	}
}

func BenchmarkRetrieveFromCache(b *testing.B) {
	ctx := context.Background()
	cache, mr := setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	tests := []struct {
		name      string
		valueSize int
		setup     func(cache *RedisCache, valueSize int) string
	}{
		{
			name:      "InMemorySmall",
			valueSize: 100,
			setup: func(cache *RedisCache, valueSize int) string {
				key := "inmem-small"
				value := generateRandomString(valueSize)
				cache.SaveInCache(key, value, time.Hour, 100*time.Millisecond)
				return key
			},
		},
		{
			name:      "InMemoryCompressed",
			valueSize: 20 * 1024, // 20KB - above compression threshold
			setup: func(cache *RedisCache, valueSize int) string {
				key := "inmem-compressed"
				value := generateRandomString(valueSize)
				cache.SaveInCache(key, value, time.Hour, 100*time.Millisecond)
				return key
			},
		},
		{
			name:      "FromRedis",
			valueSize: 100,
			setup: func(cache *RedisCache, valueSize int) string {
				key := "redis-only"
				value := generateRandomString(valueSize)
				cache.SaveInCache(key, value, time.Hour, 100*time.Millisecond)
				cache.resourceMutex.Lock()
				delete(cache.resourceCache, key)
				cache.resourceMutex.Unlock()
				return key
			},
		},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			key := tc.setup(cache, tc.valueSize)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := cache.RetrieveFromCache(key, 100*time.Millisecond)
				if err != nil {
					b.Fatalf("Error retrieving from cache: %v", err)
				}
			}
		})
	}
}

func BenchmarkConcurrentAccess(b *testing.B) {
	ctx := context.Background()
	cache, mr := setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	numGoroutines := 10
	operationsPerGoroutine := max(b.N/numGoroutines, 1)

	value := generateRandomString(100)
	for i := range 100 {
		key := makeKey("concurrent-key-", i)
		cache.SaveInCache(key, value, time.Hour, 100*time.Millisecond)
	}

	var wg sync.WaitGroup
	b.ResetTimer()

	for g := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range operationsPerGoroutine {
				op := rand.Intn(2)
				key := makeKey("concurrent-key-", rand.Intn(100))

				if op == 0 { // Read
					_, _ = cache.RetrieveFromCache(key, 100*time.Millisecond)
				} else { // Write
					_ = cache.SaveInCache(
						key,
						generateRandomString(100),
						time.Hour,
						100*time.Millisecond,
					)
				}
			}
		}(g)
	}

	wg.Wait()
}

func BenchmarkExpiry(b *testing.B) {
	ctx := context.Background()
	cache, mr := setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	tests := []struct {
		name     string
		expireIn time.Duration
		wait     time.Duration
	}{
		{"NoExpiry", 0, 0},
		{"ShortExpiry", 10 * time.Millisecond, 20 * time.Millisecond},
		{"LongExpiry", 10 * time.Second, 0},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("expiry-test-%s-%d", tc.name, i)
				err := cache.SaveInCache(key, "test-value", tc.expireIn, 100*time.Millisecond)
				if err != nil {
					b.Fatalf("Failed to save: %v", err)
				}

				if tc.wait > 0 {
					time.Sleep(tc.wait)
				}

				_, _ = cache.RetrieveFromCache(key, 100*time.Millisecond)
			}
		})
	}
}

func BenchmarkCompression(b *testing.B) {
	sizes := []int{
		5 * 1024,    // 5KB
		20 * 1024,   // 20KB
		100 * 1024,  // 100KB
		1024 * 1024, // 1MB
	}

	for _, size := range sizes {
		data := generateRandomString(size)
		b.Run(fmt.Sprintf("Compress_%dKB", size/1024), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := compressData(data)
				if err != nil {
					b.Fatalf("Compression error: %v", err)
				}
			}
		})
	}

	for _, size := range sizes {
		data := generateRandomString(size)
		compressed, err := compressData(data)
		if err != nil {
			b.Fatalf("Setup compression error: %v", err)
		}
		checksum, _ := generateChecksum(data)

		b.Run(fmt.Sprintf("Decompress_%dKB", size/1024), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := decompressData(compressed, checksum)
				if err != nil {
					b.Fatalf("Decompression error: %v", err)
				}
			}
		})
	}
}

func BenchmarkMemoryManagement(b *testing.B) {
	ctx := context.Background()
	cache, mr := setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	cache.maxCacheEntries = 50

	value := generateRandomString(100)
	b.Run("MaxEntriesEviction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := range 60 {
				key := makeKey("mem-test-", j)
				cache.SaveInCache(key, value, time.Hour, 100*time.Millisecond)
			}

			cache.resourceMutex.RLock()
			size := len(cache.resourceCache)
			cache.resourceMutex.RUnlock()

			if size > cache.maxCacheEntries {
				b.Fatalf("Cache exceeded max entries: %d > %d", size, cache.maxCacheEntries)
			}
		}
	})

	cache, mr = setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	cache.memoryCapMB = 1

	value = generateRandomString(1024)
	b.Run("MemoryCapTrimming", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := range 100 {
				key := makeKey("cap-test-", j)
				cache.SaveInCache(key, value, time.Hour, 100*time.Millisecond)
			}
			cache.doCheckMemory()
		}
	})
}
func BenchmarkSyncOperations(b *testing.B) {
	ctx := context.Background()
	cache, mr := setRedisCache(&testing.T{}, ctx)
	defer mr.Close()
	defer cache.Shutdown()

	for i := range 50 {
		key := makeKey("sync-test-", i)
		var expiry time.Duration
		if i%2 == 0 {
			expiry = 1 * time.Millisecond
		} else {
			expiry = 1 * time.Hour
		}
		cache.SaveInCache(key, generateRandomString(100), expiry, 100*time.Millisecond)
	}

	time.Sleep(10 * time.Millisecond)

	b.Run("SyncOperation", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache.doSync()
		}
	})

	b.Run("EWMAUpdate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache.updateEWMAFromSamples()
		}
	})
}

func TestCompressDecompressIntegrity(t *testing.T) {
	tests := []struct {
		name     string
		original any
	}{
		{"String", "test string data"},
		{"StringLong", generateRandomString(20 * 1024)},
		{"Int", 12345},
		{"Float", 123.456},
		{"Boolean", true},
		{"Map", map[string]any{"key1": "value1", "key2": 123}},
		{"Slice", []string{"item1", "item2", "item3"}},
		{"Struct", struct {
			Name  string
			Value int
		}{"test", 123}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := compressData(tc.original)
			require.NoError(t, err, "Compression should not fail")

			checksum, err := generateChecksum(tc.original)
			require.NoError(t, err, "Checksum generation should not fail")

			decompressed, err := decompressData(compressed, checksum)
			require.NoError(t, err, "Decompression should not fail")

			if str, ok := tc.original.(string); ok {
				require.Equal(t, str, string(decompressed), "Decompressed string should match original")
			}

			if _, ok := tc.original.(int); ok {
				require.NotNil(t, decompressed, "Decompressed data should not be nil")
			}
		})
	}
}

func generateRandomString(size int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
