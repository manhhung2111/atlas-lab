package ds

import (
	"maps"
	"sync"
)

const (
	defaultShards = 32
)

type ConcurrentHashMap[K comparable, V any] interface {
	Put(key K, value V)
	Delete(key K) bool
	Get(key K) (V, bool)
	Len() int
	Snapshot() map[K]V
}

type concurrentHashMap[K comparable, V any] struct {
	hasher func(K) uint64
	shards []shardedHashMap[K, V]
}

type shardedHashMap[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

func NewConcurrentHashMap[K comparable, V any](hasher func(K) uint64) ConcurrentHashMap[K, V] {
	if hasher == nil {
		panic("ds: NewConcurrentHashMap requires a non-nil hasher")
	}

	shards := make([]shardedHashMap[K, V], defaultShards)
	for i := range defaultShards {
		shards[i].data = make(map[K]V)
	}

	return &concurrentHashMap[K, V]{
		hasher: hasher,
		shards: shards,
	}
}

func (hashMap *concurrentHashMap[K, V]) Put(key K, value V) {
	idx := hashMap.getHashedShardIdx(key)
	shard := &hashMap.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	shard.data[key] = value
}

func (hashMap *concurrentHashMap[K, V]) Delete(key K) bool {
	idx := hashMap.getHashedShardIdx(key)
	shard := &hashMap.shards[idx]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	_, exists := shard.data[key]
	delete(shard.data, key)
	return exists
}

func (hashMap *concurrentHashMap[K, V]) Get(key K) (V, bool) {
	idx := hashMap.getHashedShardIdx(key)
	shard := &hashMap.shards[idx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	value, exists := shard.data[key]
	return value, exists
}

func (hashMap *concurrentHashMap[K, V]) Len() int {
	defer hashMap.lockAllRead()()

	totalLen := 0
	for i := range hashMap.shards {
		totalLen += len(hashMap.shards[i].data)
	}

	return totalLen
}

func (hashMap *concurrentHashMap[K, V]) Snapshot() map[K]V {
	totalLen := hashMap.Len()
	defer hashMap.lockAllRead()()

	output := make(map[K]V, totalLen)
	for i := range hashMap.shards {
		maps.Copy(output, hashMap.shards[i].data)
	}

	return output
}

func (hashMap *concurrentHashMap[K, V]) getHashedShardIdx(key K) int {
	return int(hashMap.hasher(key) % uint64(len(hashMap.shards)))
}

func (hashMap *concurrentHashMap[K, V]) lockAllRead() func() {
	for i := range hashMap.shards {
		hashMap.shards[i].mu.RLock()
	}

	return func() {
		for i := range hashMap.shards {
			hashMap.shards[i].mu.RUnlock()
		}
	}
}
