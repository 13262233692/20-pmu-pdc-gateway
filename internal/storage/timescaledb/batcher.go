package timescaledb

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/buffer"
	"github.com/wams/pmu-pdc-gateway/internal/config"
	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
	"github.com/wams/pmu-pdc-gateway/pkg/pool"
)

type BatcherStats struct {
	BatchesCreated uint64
	RowsBatched    uint64
	DroppedOnFull  uint64
}

type Batcher struct {
	cfg         config.DatabaseConfig
	sb          *buffer.ShardedBuffer
	writer      *Writer
	batchPool   *pool.PMUDataBatchPool
	pmuPool     *pool.PMUDataPool

	wg          sync.WaitGroup
	stopCh      chan struct{}
	once        sync.Once

	stats       BatcherStats
}

func NewBatcher(cfg config.DatabaseConfig, sb *buffer.ShardedBuffer,
	writer *Writer, batchPool *pool.PMUDataBatchPool, pmuPool *pool.PMUDataPool) *Batcher {
	return &Batcher{
		cfg:       cfg,
		sb:        sb,
		writer:    writer,
		batchPool: batchPool,
		pmuPool:   pmuPool,
		stopCh:    make(chan struct{}),
	}
}

func (b *Batcher) Start() {
	b.wg.Add(1)
	go b.batchLoop()
}

func (b *Batcher) Stop() {
	b.once.Do(func() {
		close(b.stopCh)
	})
	b.wg.Wait()
}

func (b *Batcher) batchLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	shardCount := b.sb.ShardCount()
	workBuf := make([]*c37118.PMUData, b.cfg.BatchSize)
	currentBatch := b.batchPool.Get()

	for {
		select {
		case <-b.stopCh:
			finalBatch := b.flushAll(currentBatch, workBuf, shardCount)
			if len(finalBatch.Data) > 0 {
				b.submitAndRecycle(finalBatch)
			} else {
				b.batchPool.Put(finalBatch)
			}
			return
		case <-ticker.C:
			currentBatch = b.flushAll(currentBatch, workBuf, shardCount)
			if len(currentBatch.Data) >= b.cfg.BatchSize {
				b.submitAndRecycle(currentBatch)
				currentBatch = b.batchPool.Get()
			}
		}
	}
}

func (b *Batcher) flushAll(currentBatch *pool.PMUDataBatch, workBuf []*c37118.PMUData, shardCount int) *pool.PMUDataBatch {
	batch := currentBatch
	for i := 0; i < shardCount; i++ {
		for {
			n := b.sb.PopBatchFromShard(i, len(workBuf), workBuf)
			if n == 0 {
				break
			}

			for j := 0; j < n; j++ {
				if len(batch.Data) >= b.cfg.BatchSize {
					b.submitAndRecycle(batch)
					batch = b.batchPool.Get()
				}
				batch.Data = append(batch.Data, workBuf[j])
				workBuf[j] = nil
			}

			atomic.AddUint64(&b.stats.RowsBatched, uint64(n))
		}
	}

	atomic.AddUint64(&b.stats.BatchesCreated, 1)
	return batch
}

func (b *Batcher) submitAndRecycle(batch *pool.PMUDataBatch) {
	dataCopy := make([]*c37118.PMUData, len(batch.Data))
	copy(dataCopy, batch.Data)

	b.writer.SubmitBatch(dataCopy)

	for _, d := range batch.Data {
		if d != nil {
			b.pmuPool.Put(d)
		}
	}

	b.batchPool.Put(batch)
}

func (b *Batcher) Stats() BatcherStats {
	return BatcherStats{
		BatchesCreated: atomic.LoadUint64(&b.stats.BatchesCreated),
		RowsBatched:    atomic.LoadUint64(&b.stats.RowsBatched),
		DroppedOnFull:  atomic.LoadUint64(&b.stats.DroppedOnFull),
	}
}
