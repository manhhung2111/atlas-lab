package ds

import (
	"fmt"
	"hash/fnv"
	"math"
	"sync"
	"testing"
)

func fnvString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func newStringMap() ConcurrentHashMap[string, int] {
	return NewConcurrentHashMap[string, int](fnvString)
}

func TestPutGet(t *testing.T) {
	m := newStringMap()
	m.Put("a", 1)

	if v, ok := m.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %d, %v; want 1, true", v, ok)
	}
}

func TestPutOverwrites(t *testing.T) {
	m := newStringMap()
	m.Put("a", 1)
	m.Put("a", 2)

	if v, _ := m.Get("a"); v != 2 {
		t.Fatalf("Get(a) = %d; want 2", v)
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len() = %d; want 1", got)
	}
}

func TestGetMissingReturnsZeroAndFalse(t *testing.T) {
	m := newStringMap()

	if v, ok := m.Get("missing"); ok || v != 0 {
		t.Fatalf("Get(missing) = %d, %v; want 0, false", v, ok)
	}
}

func TestGetDistinguishesStoredZeroFromMissing(t *testing.T) {
	m := newStringMap()
	m.Put("zero", 0)

	if v, ok := m.Get("zero"); !ok || v != 0 {
		t.Fatalf("Get(zero) = %d, %v; want 0, true", v, ok)
	}
}

func TestDelete(t *testing.T) {
	m := newStringMap()
	m.Put("a", 1)

	if !m.Delete("a") {
		t.Fatal("Delete(a) = false; want true")
	}
	if m.Delete("a") {
		t.Fatal("second Delete(a) = true; want false")
	}
	if _, ok := m.Get("a"); ok {
		t.Fatal("Get(a) found key after Delete")
	}
}

func TestLenAndSnapshot(t *testing.T) {
	m := newStringMap()
	want := map[string]int{}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%d", i)
		m.Put(k, i)
		want[k] = i
	}

	if got := m.Len(); got != len(want) {
		t.Fatalf("Len() = %d; want %d", got, len(want))
	}

	snap := m.Snapshot()
	if len(snap) != len(want) {
		t.Fatalf("len(Snapshot()) = %d; want %d", len(snap), len(want))
	}
	for k, v := range want {
		if snap[k] != v {
			t.Fatalf("Snapshot()[%s] = %d; want %d", k, snap[k], v)
		}
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	m := newStringMap()
	m.Put("a", 1)

	snap := m.Snapshot()
	snap["a"] = 99
	snap["b"] = 2

	if v, _ := m.Get("a"); v != 1 {
		t.Fatalf("mutating snapshot changed map: Get(a) = %d; want 1", v)
	}
	if _, ok := m.Get("b"); ok {
		t.Fatal("key added to snapshot appeared in map")
	}

	m.Put("c", 3)
	if _, ok := snap["c"]; ok {
		t.Fatal("later Put appeared in earlier snapshot")
	}
}

func TestEmptyMap(t *testing.T) {
	m := newStringMap()

	if got := m.Len(); got != 0 {
		t.Fatalf("Len() = %d; want 0", got)
	}
	if got := len(m.Snapshot()); got != 0 {
		t.Fatalf("len(Snapshot()) = %d; want 0", got)
	}
}

func TestNilHasherPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil hasher")
		}
	}()

	NewConcurrentHashMap[string, int](nil)
}

// Hash values above math.MaxInt64 must still map to a valid shard index.
func TestShardIndexInRange(t *testing.T) {
	for _, h := range []uint64{0, 1, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		m := NewConcurrentHashMap[string, int](func(string) uint64 { return h })
		m.Put("k", 1)

		if v, ok := m.Get("k"); !ok || v != 1 {
			t.Fatalf("hash %d: Get(k) = %d, %v; want 1, true", h, v, ok)
		}
	}
}

func TestKeysSpreadAcrossShards(t *testing.T) {
	m := newStringMap().(*concurrentHashMap[string, int])
	for i := 0; i < 10000; i++ {
		m.Put(fmt.Sprintf("key-%d", i), i)
	}

	for i := range m.shards {
		if len(m.shards[i].data) == 0 {
			t.Fatalf("shard %d is empty; hashing does not spread keys", i)
		}
	}
}

func TestConcurrentPutGetDelete(t *testing.T) {
	const goroutines, perGoroutine = 16, 2000

	m := newStringMap()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				k := fmt.Sprintf("g%d-%d", g, i)
				m.Put(k, i)
				if v, ok := m.Get(k); !ok || v != i {
					t.Errorf("Get(%s) = %d, %v; want %d, true", k, v, ok, i)
					return
				}
				if i%2 == 0 && !m.Delete(k) {
					t.Errorf("Delete(%s) = false; want true", k)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got, want := m.Len(), goroutines*perGoroutine/2; got != want {
		t.Fatalf("Len() = %d; want %d", got, want)
	}
}

// Len and Snapshot take every shard lock, so run them against live writers
// to catch deadlocks (and data races under -race).
func TestConcurrentLenSnapshotWithWriters(t *testing.T) {
	const writers, perWriter = 8, 2000

	m := newStringMap()
	stop := make(chan struct{})

	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m.Len()
					m.Snapshot()
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("w%d-%d", w, i)
				m.Put(k, i)
				if i%4 == 0 {
					m.Delete(k)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	readers.Wait()

	want := writers * (perWriter - perWriter/4)
	if got := m.Len(); got != want {
		t.Fatalf("Len() = %d; want %d", got, want)
	}
	if got := len(m.Snapshot()); got != want {
		t.Fatalf("len(Snapshot()) = %d; want %d", got, want)
	}
}
