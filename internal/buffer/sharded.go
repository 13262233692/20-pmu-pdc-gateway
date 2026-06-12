package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type ShardedBuffer struct {
	shards    []*RingBuffer
	shardMask uint32
	policy    DropPolicy
	capacity  int

	stats Stats
}

func NewShardedBuffer(shardCount, capacity int, policy DropPolicy) *ShardedBuffer {
	sc := nextPowerOfTwo(shardCount)
	if sc < 1 {
		sc = 1
	}
	sb := &ShardedBuffer{
		shards:    make([]*RingBuffer, sc),
		shardMask: uint32(sc - 1),
		policy:    policy,
		capacity:  capacity,
	}
	for i := 0; i < sc; i++ {
		sb.shards[i] = NewRingBuffer(capacity, policy)
	}
	return sb
}

func (sb *ShardedBuffer) shardIndex(id uint16) uint32 {
	h := uint32(id)*2654435761 + 0x9e3779b9
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h & sb.shardMask
}

func (sb *ShardedBuffer) Push(d *c37118.PMUData) bool {
	atomic.AddUint64(&sb.stats.Incoming, 1)
	idx := sb.shardIndex(d.IDCode)
	ok := sb.shards[idx].Push(d)
	if !ok {
		atomic.AddUint64(&sb.stats.Dropped, 1)
	}
	return ok
}

func (sb *ShardedBuffer) ShardCount() int {
	return len(sb.shards)
}

func (sb *ShardedBuffer) Shard(idx int) *RingBuffer {
	if idx < 0 || idx >= len(sb.shards) {
		return nil
	}
	return sb.shards[idx]
}

func (sb *ShardedBuffer) PopBatchFromShard(idx int, max int, out []*c37118.PMUData) int {
	if idx < 0 || idx >= len(sb.shards) {
		return 0
	}
	return sb.shards[idx].PopBatch(max, out)
}

func (sb *ShardedBuffer) TotalLen() int {
	total := 0
	for _, s := range sb.shards {
		total += s.Len()
	}
	return total
}

func (sb *ShardedBuffer) Stats() Stats {
	var result Stats
	result.Incoming = atomic.LoadUint64(&sb.stats.Incoming)
	result.Dropped = atomic.LoadUint64(&sb.stats.Dropped)
	for _, s := range sb.shards {
		ss := s.Stats()
		result.Flushed += ss.Flushed
		result.Overflow += ss.Overflow
	}
	return result
}

func (sb *ShardedBuffer) PerShardStats() []Stats {
	result := make([]Stats, len(sb.shards))
	for i, s := range sb.shards {
		result[i] = s.Stats()
	}
	return result
}

func nextPowerOfTwo(n int) int {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}

type FlushCallback func(batch []*c37118.PMUData)

type ConcurrentFlusher struct {
	sb          *ShardedBuffer
	batchSize   int
	workers     int
	callback    FlushCallback
	wg          sync.WaitGroup
	stopCh      chan struct{}
	once        sync.Once
}

func NewConcurrentFlusher(sb *ShardedBuffer, batchSize, workers int, cb FlushCallback) *ConcurrentFlusher {
	return &ConcurrentFlusher{
		sb:        sb,
		batchSize: batchSize,
		workers:   workers,
		callback:  cb,
		stopCh:    make(chan struct{}),
	}
}

func (cf *ConcurrentFlusher) Start() {
	for i := 0; i < cf.workers; i++ {
		cf.wg.Add(1)
		go cf.worker()
	}
}

func (cf *ConcurrentFlusher) Stop() {
	cf.once.Do(func() {
		close(cf.stopCh)
	})
	cf.wg.Wait()
}

func (cf *ConcurrentFlusher) worker() {
	defer cf.wg.Done()
	batch := make([]*c37118.PMUData, cf.batchSize)
	shardCount := cf.sb.ShardCount()

	for {
		select {
		case <-cf.stopCh:
			cf.flushRemaining(batch)
			return
		default:
		}

		flushed := false
		for i := 0; i < shardCount; i++ {
			n := cf.sb.PopBatchFromShard(i, cf.batchSize, batch)
			if n > 0 {
				cf.callback(batch[:n])
				flushed = true
			}
		}

		if !flushed {
			select {
			case <-cf.stopCh:
				cf.flushRemaining(batch)
				return
			default:
			}
		}
	}
}

func (cf *ConcurrentFlusher) flushRemaining(batch []*c37118.PMUData) {
	shardCount := cf.sb.ShardCount()
	for i := 0; i < shardCount; i++ {
		for {
			n := cf.sb.PopBatchFromShard(i, cf.batchSize, batch)
			if n == 0 {
				break
			}
			cf.callback(batch[:n])
		}
	}
}
