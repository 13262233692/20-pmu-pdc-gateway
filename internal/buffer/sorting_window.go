package buffer

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type Slot struct {
	deadline  time.Time
	arrival   time.Time
	data      *c37118.PMUData
	seq       uint64
}

type WindowStats struct {
	Inserted      uint64
	Evicted       uint64
	Expired       uint64
	ForceEvicted  uint64
	BackwardDrops uint64
	CurrentSlots  int64
	TTLBreaches   uint64
}

type SortingWindow struct {
	mu          sync.Mutex
	slots       []Slot
	maxSlots    int
	ttl         time.Duration
	windowWidth time.Duration
	nextSeq     uint64
	validator   *TimestampValidator
	onEvict     func(*c37118.PMUData)
	stats       WindowStats
}

func NewSortingWindow(maxSlots int, ttl, windowWidth time.Duration,
	validator *TimestampValidator, onEvict func(*c37118.PMUData)) *SortingWindow {
	return &SortingWindow{
		slots:       make([]Slot, 0, maxSlots),
		maxSlots:    maxSlots,
		ttl:         ttl,
		windowWidth: windowWidth,
		validator:   validator,
		onEvict:     onEvict,
	}
}

func (sw *SortingWindow) Insert(d *c37118.PMUData) {
	now := time.Now()

	if sw.validator != nil {
		verdict := sw.validator.Validate(d)
		if !ApplyVerdict(d, verdict) {
			atomic.AddUint64(&sw.stats.BackwardDrops, 1)
			return
		}
	}

	sw.mu.Lock()

	sw.slots = append(sw.slots, Slot{
		deadline: now.Add(sw.ttl),
		arrival:  now,
		data:     d,
		seq:      sw.nextSeq,
	})
	sw.nextSeq++
	atomic.AddUint64(&sw.stats.Inserted, 1)
	atomic.AddInt64(&sw.stats.CurrentSlots, 1)

	sw.maybeEvictLocked(now)
	sw.mu.Unlock()
}

func (sw *SortingWindow) maybeEvictLocked(now time.Time) {
	if len(sw.slots) == 0 {
		return
	}

	watermark := now.Add(-sw.windowWidth)
	oldest := sw.slots[0].arrival
	if oldest.After(watermark) && len(sw.slots) < sw.maxSlots {
		return
	}

	sw.evictLocked(now)
}

func (sw *SortingWindow) evictLocked(now time.Time) {
	if len(sw.slots) == 0 {
		return
	}

	watermark := now.Add(-sw.windowWidth)

	evictEnd := 0
	for i, s := range sw.slots {
		if s.arrival.After(watermark) && s.deadline.After(now) {
			break
		}
		evictEnd = i + 1
	}

	if evictEnd == 0 {
		if len(sw.slots) >= sw.maxSlots {
			evictEnd = len(sw.slots) / 4
			if evictEnd < 1 {
				evictEnd = 1
			}
			atomic.AddUint64(&sw.stats.ForceEvicted, uint64(evictEnd))
		} else {
			return
		}
	}

	toEvict := sw.slots[:evictEnd]

	sort.Slice(toEvict, func(i, j int) bool {
		if toEvict[i].data.Timestamp != toEvict[j].data.Timestamp {
			return toEvict[i].data.Timestamp < toEvict[j].data.Timestamp
		}
		return toEvict[i].seq < toEvict[j].seq
	})

	for _, s := range toEvict {
		if !s.deadline.After(now) {
			atomic.AddUint64(&sw.stats.Expired, 1)
			atomic.AddUint64(&sw.stats.TTLBreaches, 1)
		} else {
			atomic.AddUint64(&sw.stats.Evicted, 1)
		}
		atomic.AddInt64(&sw.stats.CurrentSlots, -1)

		if sw.onEvict != nil && s.data != nil {
			sw.onEvict(s.data)
		}
	}

	remaining := sw.slots[evictEnd:]
	sw.slots = make([]Slot, len(remaining), cap(sw.slots))
	copy(sw.slots, remaining)
}

func (sw *SortingWindow) ForceExpireAll() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	n := len(sw.slots)
	sort.Slice(sw.slots, func(i, j int) bool {
		if sw.slots[i].data.Timestamp != sw.slots[j].data.Timestamp {
			return sw.slots[i].data.Timestamp < sw.slots[j].data.Timestamp
		}
		return sw.slots[i].seq < sw.slots[j].seq
	})

	for _, s := range sw.slots {
		atomic.AddUint64(&sw.stats.Expired, 1)
		atomic.AddUint64(&sw.stats.ForceEvicted, 1)
		atomic.AddInt64(&sw.stats.CurrentSlots, -1)
		if sw.onEvict != nil && s.data != nil {
			sw.onEvict(s.data)
		}
	}
	sw.slots = sw.slots[:0]
	return n
}

func (sw *SortingWindow) RunTTLScanner(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	pruneTicker := time.NewTicker(5 * time.Minute)
	defer pruneTicker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case now := <-ticker.C:
			sw.mu.Lock()
			sw.evictLocked(now)
			sw.mu.Unlock()
		case <-pruneTicker.C:
			if sw.validator != nil {
				sw.validator.PruneStalePMUs(10 * time.Minute)
			}
		}
	}
}

func (sw *SortingWindow) Len() int {
	sw.mu.Lock()
	n := len(sw.slots)
	sw.mu.Unlock()
	return n
}

func (sw *SortingWindow) Stats() WindowStats {
	sw.mu.Lock()
	current := int64(len(sw.slots))
	sw.mu.Unlock()

	return WindowStats{
		Inserted:      atomic.LoadUint64(&sw.stats.Inserted),
		Evicted:       atomic.LoadUint64(&sw.stats.Evicted),
		Expired:       atomic.LoadUint64(&sw.stats.Expired),
		ForceEvicted:  atomic.LoadUint64(&sw.stats.ForceEvicted),
		BackwardDrops: atomic.LoadUint64(&sw.stats.BackwardDrops),
		CurrentSlots:  current,
		TTLBreaches:   atomic.LoadUint64(&sw.stats.TTLBreaches),
	}
}

type ringSlot struct {
	data       *c37118.PMUData
	deadlineNs int64
	arrivalNs  int64
	ts         uint64
	seq        uint64
}

type LockFreeRingWindow struct {
	mu        sync.Mutex
	ring      []ringSlot
	capacity  uint32
	mask      uint32
	ttl       time.Duration
	writePos  uint64

	validator *TimestampValidator
	onEvict   func(*c37118.PMUData)

	stats WindowStats
}

func NewLockFreeRingWindow(capacity int, ttl time.Duration,
	validator *TimestampValidator, onEvict func(*c37118.PMUData)) *LockFreeRingWindow {
	c := nextPowerOfTwo(capacity)
	return &LockFreeRingWindow{
		ring:      make([]ringSlot, c),
		capacity:  uint32(c),
		mask:      uint32(c - 1),
		ttl:       ttl,
		validator: validator,
		onEvict:   onEvict,
	}
}

func (rfw *LockFreeRingWindow) Insert(d *c37118.PMUData) {
	now := time.Now()

	if rfw.validator != nil {
		verdict := rfw.validator.Validate(d)
		if !ApplyVerdict(d, verdict) {
			atomic.AddUint64(&rfw.stats.BackwardDrops, 1)
			return
		}
	}

	rfw.mu.Lock()

	pos := rfw.writePos
	rfw.writePos++

	idx := uint32(pos) & rfw.mask
	s := &rfw.ring[idx]

	if s.data != nil {
		if rfw.onEvict != nil {
			rfw.onEvict(s.data)
		}
		atomic.AddUint64(&rfw.stats.Evicted, 1)
		atomic.AddInt64(&rfw.stats.CurrentSlots, -1)
	}

	s.data = d
	s.ts = d.Timestamp
	s.arrivalNs = now.UnixNano()
	s.deadlineNs = now.Add(rfw.ttl).UnixNano()
	s.seq = pos

	atomic.AddUint64(&rfw.stats.Inserted, 1)
	atomic.AddInt64(&rfw.stats.CurrentSlots, 1)

	rfw.mu.Unlock()
}

func (rfw *LockFreeRingWindow) ForceExpire() int {
	rfw.mu.Lock()
	defer rfw.mu.Unlock()

	now := time.Now().UnixNano()
	expired := 0

	for i := uint32(0); i < rfw.capacity; i++ {
		s := &rfw.ring[i]
		if s.data == nil {
			continue
		}

		if s.deadlineNs <= now {
			if rfw.onEvict != nil {
				rfw.onEvict(s.data)
			}
			s.data = nil
			atomic.AddUint64(&rfw.stats.Expired, 1)
			atomic.AddUint64(&rfw.stats.TTLBreaches, 1)
			atomic.AddInt64(&rfw.stats.CurrentSlots, -1)
			expired++
		}
	}

	return expired
}

func (rfw *LockFreeRingWindow) ExpireAndSortDrain() []*c37118.PMUData {
	rfw.mu.Lock()
	defer rfw.mu.Unlock()

	now := time.Now().UnixNano()
	var result []*c37118.PMUData

	for i := uint32(0); i < rfw.capacity; i++ {
		s := &rfw.ring[i]
		if s.data == nil {
			continue
		}

		if s.deadlineNs <= now || s.arrivalNs <= now-rfw.ttl.Nanoseconds() {
			result = append(result, s.data)
			s.data = nil
			atomic.AddUint64(&rfw.stats.Expired, 1)
			atomic.AddInt64(&rfw.stats.CurrentSlots, -1)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp < result[j].Timestamp
	})

	return result
}

func (rfw *LockFreeRingWindow) RunTTLScanner(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	pruneTicker := time.NewTicker(5 * time.Minute)
	defer pruneTicker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			rfw.ForceExpire()
		case <-pruneTicker.C:
			if rfw.validator != nil {
				rfw.validator.PruneStalePMUs(10 * time.Minute)
			}
		}
	}
}

func (rfw *LockFreeRingWindow) ActiveCount() int {
	rfw.mu.Lock()
	count := 0
	for i := uint32(0); i < rfw.capacity; i++ {
		if rfw.ring[i].data != nil {
			count++
		}
	}
	rfw.mu.Unlock()
	return count
}

func (rfw *LockFreeRingWindow) Stats() WindowStats {
	return WindowStats{
		Inserted:      atomic.LoadUint64(&rfw.stats.Inserted),
		Evicted:       atomic.LoadUint64(&rfw.stats.Evicted),
		Expired:       atomic.LoadUint64(&rfw.stats.Expired),
		ForceEvicted:  atomic.LoadUint64(&rfw.stats.ForceEvicted),
		BackwardDrops: atomic.LoadUint64(&rfw.stats.BackwardDrops),
		CurrentSlots:  int64(rfw.ActiveCount()),
		TTLBreaches:   atomic.LoadUint64(&rfw.stats.TTLBreaches),
	}
}
