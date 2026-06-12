package analytics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type FreqSample struct {
	Timestamp uint64
	Freq      float64
}

type PMUFreqWindow struct {
	mu       sync.Mutex
	samples  []FreqSample
	capacity int
	pmuID    uint16
	lastTS   uint64
}

func NewPMUFreqWindow(pmuID uint16, capacity int) *PMUFreqWindow {
	return &PMUFreqWindow{
		pmuID:    pmuID,
		samples:  make([]FreqSample, 0, capacity),
		capacity: capacity,
	}
}

func (w *PMUFreqWindow) Push(s FreqSample) {
	w.mu.Lock()

	if s.Timestamp <= w.lastTS {
		w.mu.Unlock()
		return
	}
	w.lastTS = s.Timestamp

	if len(w.samples) >= w.capacity {
		copy(w.samples, w.samples[1:])
		w.samples = w.samples[:len(w.samples)-1]
	}
	w.samples = append(w.samples, s)

	w.mu.Unlock()
}

func (w *PMUFreqWindow) Len() int {
	w.mu.Lock()
	n := len(w.samples)
	w.mu.Unlock()
	return n
}

func (w *PMUFreqWindow) Snapshot() []FreqSample {
	w.mu.Lock()
	out := make([]FreqSample, len(w.samples))
	copy(out, w.samples)
	w.mu.Unlock()
	return out
}

func (w *PMUFreqWindow) FreqValues() []float64 {
	w.mu.Lock()
	out := make([]float64, len(w.samples))
	for i, s := range w.samples {
		out[i] = s.Freq
	}
	w.mu.Unlock()
	return out
}

func (w *PMUFreqWindow) Timestamps() []uint64 {
	w.mu.Lock()
	out := make([]uint64, len(w.samples))
	for i, s := range w.samples {
		out[i] = s.Timestamp
	}
	w.mu.Unlock()
	return out
}

func (w *PMUFreqWindow) IsFull() bool {
	w.mu.Lock()
	full := len(w.samples) >= w.capacity
	w.mu.Unlock()
	return full
}

func (w *PMUFreqWindow) Duration() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.samples) < 2 {
		return 0
	}
	first := w.samples[0].Timestamp
	last := w.samples[len(w.samples)-1].Timestamp
	return time.Duration(last-first) * time.Nanosecond
}

type FreqWindowManager struct {
	mu         sync.RWMutex
	windows    map[uint16]*PMUFreqWindow
	capacity   int
	windowDur  time.Duration
	sampleRate int

	totalSamples uint64
}

func NewFreqWindowManager(capacity int, sampleRate int, windowDur time.Duration) *FreqWindowManager {
	return &FreqWindowManager{
		windows:    make(map[uint16]*PMUFreqWindow),
		capacity:   capacity,
		windowDur:  windowDur,
		sampleRate: sampleRate,
	}
}

func (m *FreqWindowManager) HandlePMUData(d *c37118.PMUData) {
	atomic.AddUint64(&m.totalSamples, 1)

	m.mu.RLock()
	w, exists := m.windows[d.IDCode]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		if w, exists = m.windows[d.IDCode]; !exists {
			w = NewPMUFreqWindow(d.IDCode, m.capacity)
			m.windows[d.IDCode] = w
		}
		m.mu.Unlock()
	}

	w.Push(FreqSample{
		Timestamp: d.Timestamp,
		Freq:      d.Freq,
	})
}

func (m *FreqWindowManager) GetWindow(pmuID uint16) (*PMUFreqWindow, bool) {
	m.mu.RLock()
	w, ok := m.windows[pmuID]
	m.mu.RUnlock()
	return w, ok
}

func (m *FreqWindowManager) PMUCount() int {
	m.mu.RLock()
	n := len(m.windows)
	m.mu.RUnlock()
	return n
}

func (m *FreqWindowManager) AllWindows() []*PMUFreqWindow {
	m.mu.RLock()
	out := make([]*PMUFreqWindow, 0, len(m.windows))
	for _, w := range m.windows {
		out = append(out, w)
	}
	m.mu.RUnlock()
	return out
}

func (m *FreqWindowManager) ReadyWindows() []*PMUFreqWindow {
	m.mu.RLock()
	out := make([]*PMUFreqWindow, 0, len(m.windows))
	for _, w := range m.windows {
		if w.IsFull() {
			out = append(out, w)
		}
	}
	m.mu.RUnlock()
	return out
}

func (m *FreqWindowManager) TotalSamples() uint64 {
	return atomic.LoadUint64(&m.totalSamples)
}

func (m *FreqWindowManager) PruneStale(maxAge time.Duration, nowTS uint64) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	pruned := 0
	threshold := nowTS - uint64(maxAge.Nanoseconds())

	for id, w := range m.windows {
		w.mu.Lock()
		lastTS := w.lastTS
		w.mu.Unlock()
		if lastTS < threshold {
			delete(m.windows, id)
			pruned++
		}
	}
	return pruned
}
