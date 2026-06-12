package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type Stats struct {
	Incoming    uint64
	Dropped     uint64
	Flushed     uint64
	Overflow    uint64
}

type DropPolicy int

const (
	DropOldest DropPolicy = iota
	DropNewest
	DropBlock
)

type RingBuffer struct {
	mu       sync.Mutex
	notFull  *sync.Cond
	notEmpty *sync.Cond

	data     []*c37118.PMUData
	capacity int
	head     int
	tail     int
	count    int
	dropPolicy DropPolicy

	stats Stats
}

func NewRingBuffer(capacity int, policy DropPolicy) *RingBuffer {
	rb := &RingBuffer{
		data:       make([]*c37118.PMUData, capacity),
		capacity:   capacity,
		dropPolicy: policy,
	}
	rb.notFull = sync.NewCond(&rb.mu)
	rb.notEmpty = sync.NewCond(&rb.mu)
	return rb
}

func (rb *RingBuffer) Push(d *c37118.PMUData) bool {
	rb.mu.Lock()

	for rb.count == rb.capacity {
		switch rb.dropPolicy {
		case DropOldest:
			rb.data[rb.head] = nil
			rb.head = (rb.head + 1) % rb.capacity
			rb.count--
			atomic.AddUint64(&rb.stats.Dropped, 1)
		case DropNewest:
			atomic.AddUint64(&rb.stats.Dropped, 1)
			rb.mu.Unlock()
			return false
		case DropBlock:
			rb.notFull.Wait()
		}
	}

	rb.data[rb.tail] = d
	rb.tail = (rb.tail + 1) % rb.capacity
	rb.count++
	atomic.AddUint64(&rb.stats.Incoming, 1)

	rb.mu.Unlock()
	rb.notEmpty.Signal()
	return true
}

func (rb *RingBuffer) Pop() (*c37118.PMUData, bool) {
	rb.mu.Lock()
	for rb.count == 0 {
		rb.mu.Unlock()
		return nil, false
	}

	d := rb.data[rb.head]
	rb.data[rb.head] = nil
	rb.head = (rb.head + 1) % rb.capacity
	rb.count--
	atomic.AddUint64(&rb.stats.Flushed, 1)

	rb.mu.Unlock()
	rb.notFull.Signal()
	return d, true
}

func (rb *RingBuffer) PopBatch(max int, out []*c37118.PMUData) int {
	rb.mu.Lock()

	n := rb.count
	if n > max {
		n = max
	}
	if n == 0 {
		rb.mu.Unlock()
		return 0
	}

	end := rb.head + n
	if end <= rb.capacity {
		copy(out, rb.data[rb.head:end])
		for i := rb.head; i < end; i++ {
			rb.data[i] = nil
		}
	} else {
		first := rb.capacity - rb.head
		copy(out, rb.data[rb.head:rb.capacity])
		copy(out[first:], rb.data[0:n-first])
		for i := rb.head; i < rb.capacity; i++ {
			rb.data[i] = nil
		}
		for i := 0; i < n-first; i++ {
			rb.data[i] = nil
		}
	}

	rb.head = (rb.head + n) % rb.capacity
	rb.count -= n
	atomic.AddUint64(&rb.stats.Flushed, uint64(n))

	rb.mu.Unlock()
	rb.notFull.Broadcast()
	return n
}

func (rb *RingBuffer) Len() int {
	rb.mu.Lock()
	n := rb.count
	rb.mu.Unlock()
	return n
}

func (rb *RingBuffer) Cap() int {
	return rb.capacity
}

func (rb *RingBuffer) Stats() Stats {
	return Stats{
		Incoming: atomic.LoadUint64(&rb.stats.Incoming),
		Dropped:  atomic.LoadUint64(&rb.stats.Dropped),
		Flushed:  atomic.LoadUint64(&rb.stats.Flushed),
		Overflow: atomic.LoadUint64(&rb.stats.Overflow),
	}
}
