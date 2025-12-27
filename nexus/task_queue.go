package nexus

import (
	"context"
	"encoding/json"
	"grain/cache"
)

type RedisTaskQueue struct {
	client   *cache.RedisCache
	queueKey string
}

func NewRedisTaskQueue(client *cache.RedisCache, queueKey string) *RedisTaskQueue {
	return &RedisTaskQueue{client: client, queueKey: queueKey}
}

func (r *RedisTaskQueue) PushNewTaskState(state any) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.client.InternalCache.RPush(context.Background(), r.queueKey, data).Err()
}

func (r *RedisTaskQueue) PullNewTaskState(ctx context.Context) (any, error) {
	result, err := r.client.InternalCache.LPop(ctx, r.queueKey).Result()
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

func (r *RedisTaskQueue) IsEmpty(ctx context.Context) bool {
	length, err := r.client.InternalCache.LLen(ctx, r.queueKey).Result()
	return err == nil && length == 0
}
