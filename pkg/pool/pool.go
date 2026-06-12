package pool

import (
	"sync"

	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type ByteBufferPool struct {
	pools []sync.Pool
	sizes []int
}

func NewByteBufferPool(sizes ...int) *ByteBufferPool {
	p := &ByteBufferPool{
		sizes: sizes,
		pools: make([]sync.Pool, len(sizes)),
	}
	for i, size := range sizes {
		s := size
		p.pools[i].New = func() interface{} {
			return make([]byte, s)
		}
	}
	return p
}

func (p *ByteBufferPool) Get(needed int) []byte {
	for i, size := range p.sizes {
		if size >= needed {
			buf := p.pools[i].Get().([]byte)
			return buf[:needed]
		}
	}
	return make([]byte, needed)
}

func (p *ByteBufferPool) Put(buf []byte) {
	c := cap(buf)
	for i, size := range p.sizes {
		if c == size {
			p.pools[i].Put(buf[:size])
			return
		}
	}
}

type PMUDataPool struct {
	pool        sync.Pool
	phasorCount int
	analogCount int
	digitalCount int
}

func NewPMUDataPool(phasorCount, analogCount, digitalCount int) *PMUDataPool {
	p := &PMUDataPool{
		phasorCount: phasorCount,
		analogCount: analogCount,
		digitalCount: digitalCount,
	}
	p.pool.New = func() interface{} {
		return &c37118.PMUData{
			Phasors:  make([]c37118.Phasor, phasorCount),
			Analogs:  make([]float64, analogCount),
			Digitals: make([]uint16, digitalCount),
		}
	}
	return p
}

func (p *PMUDataPool) Get() *c37118.PMUData {
	return p.pool.Get().(*c37118.PMUData)
}

func (p *PMUDataPool) Put(d *c37118.PMUData) {
	p.pool.Put(d)
}

type PMUDataBatch struct {
	Data  []*c37118.PMUData
	Pool  *PMUDataBatchPool
}

func (b *PMUDataBatch) Reset() {
	for i := range b.Data {
		b.Data[i] = nil
	}
	b.Data = b.Data[:0]
}

func (b *PMUDataBatch) Release() {
	if b.Pool != nil {
		b.Pool.Put(b)
	}
}

type PMUDataBatchPool struct {
	pool    sync.Pool
	batchSize int
}

func NewPMUDataBatchPool(batchSize int) *PMUDataBatchPool {
	p := &PMUDataBatchPool{batchSize: batchSize}
	p.pool.New = func() interface{} {
		return &PMUDataBatch{
			Data: make([]*c37118.PMUData, 0, batchSize),
			Pool: p,
		}
	}
	return p
}

func (p *PMUDataBatchPool) Get() *PMUDataBatch {
	b := p.pool.Get().(*PMUDataBatch)
	b.Reset()
	return b
}

func (p *PMUDataBatchPool) Put(b *PMUDataBatch) {
	b.Reset()
	p.pool.Put(b)
}
