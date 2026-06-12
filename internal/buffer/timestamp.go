package buffer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type TimestampAction int

const (
	TimestampAccept  TimestampAction = iota
	TimestampClamp
	TimestampReject
)

type TimestampVerdict struct {
	Action      TimestampAction
	CorrectedTS uint64
	Drift       time.Duration
}

type TimestampStats struct {
	Total       uint64
	Accepted    uint64
	Clamped     uint64
	Rejected    uint64
	MaxDriftNs  int64
	Backward    uint64
}

type TimestampValidator struct {
	mu             sync.Mutex
	lastByPMU      map[uint64]uint64
	maxForwardDrift time.Duration
	maxBackwardStep time.Duration
	frameInterval  time.Duration
	clock          func() time.Time

	stats          TimestampStats
}

func NewTimestampValidator(maxForwardDrift, maxBackwardStep, frameInterval time.Duration) *TimestampValidator {
	tv := &TimestampValidator{
		lastByPMU:      make(map[uint64]uint64),
		maxForwardDrift: maxForwardDrift,
		maxBackwardStep: maxBackwardStep,
		frameInterval:  frameInterval,
		clock:          time.Now,
	}
	return tv
}

func (tv *TimestampValidator) Validate(d *c37118.PMUData) TimestampVerdict {
	atomic.AddUint64(&tv.stats.Total, 1)

	ts := d.Timestamp
	now := tv.clock().UnixNano()
	key := uint64(d.IDCode)

	tv.mu.Lock()
	lastTS, hasLast := tv.lastByPMU[key]
	tv.lastByPMU[key] = ts
	tv.mu.Unlock()

	verdict := TimestampVerdict{Action: TimestampAccept, CorrectedTS: ts}

	wallDrift := int64(ts) - now
	if wallDrift < 0 {
		wallDrift = -wallDrift
	}
	if time.Duration(wallDrift) > tv.maxForwardDrift {
		if time.Duration(wallDrift) > tv.maxForwardDrift*2 {
			atomic.AddUint64(&tv.stats.Rejected, 1)
			verdict.Action = TimestampReject
			verdict.Drift = time.Duration(wallDrift)
			return verdict
		}
		clamped := uint64(now)
		verdict.Action = TimestampClamp
		verdict.CorrectedTS = clamped
		verdict.Drift = time.Duration(wallDrift)
		atomic.AddUint64(&tv.stats.Clamped, 1)

		tv.mu.Lock()
		tv.lastByPMU[key] = clamped
		tv.mu.Unlock()
		return verdict
	}

	if hasLast {
		backwardStep := int64(lastTS) - int64(ts)
		if backwardStep > 0 {
			if time.Duration(backwardStep) > tv.maxBackwardStep {
				atomic.AddUint64(&tv.stats.Backward, 1)
				clamped := lastTS + uint64(tv.frameInterval.Nanoseconds())
				verdict.Action = TimestampClamp
				verdict.CorrectedTS = clamped
				verdict.Drift = -time.Duration(backwardStep)

				tv.mu.Lock()
				tv.lastByPMU[key] = clamped
				tv.mu.Unlock()

				atomic.AddUint64(&tv.stats.Clamped, 1)
				return verdict
			}
		}

		forwardStep := int64(ts) - int64(lastTS)
		if forwardStep > 0 && time.Duration(forwardStep) > tv.maxForwardDrift {
			clamped := lastTS + uint64(tv.frameInterval.Nanoseconds())
			verdict.Action = TimestampClamp
			verdict.CorrectedTS = clamped
			verdict.Drift = time.Duration(forwardStep)

			tv.mu.Lock()
			tv.lastByPMU[key] = clamped
			tv.mu.Unlock()

			atomic.AddUint64(&tv.stats.Clamped, 1)
			return verdict
		}
	}

	atomic.AddUint64(&tv.stats.Accepted, 1)
	return verdict
}

func (tv *TimestampValidator) PruneStalePMUs(maxAge time.Duration) int {
	tv.mu.Lock()
	defer tv.mu.Unlock()

	now := tv.clock().UnixNano()
	threshold := uint64(now - maxAge.Nanoseconds())
	pruned := 0

	for key, lastTS := range tv.lastByPMU {
		if lastTS < threshold {
			delete(tv.lastByPMU, key)
			pruned++
		}
	}
	return pruned
}

func (tv *TimestampValidator) Stats() TimestampStats {
	return TimestampStats{
		Total:      atomic.LoadUint64(&tv.stats.Total),
		Accepted:   atomic.LoadUint64(&tv.stats.Accepted),
		Clamped:    atomic.LoadUint64(&tv.stats.Clamped),
		Rejected:   atomic.LoadUint64(&tv.stats.Rejected),
		MaxDriftNs: atomic.LoadInt64(&tv.stats.MaxDriftNs),
		Backward:   atomic.LoadUint64(&tv.stats.Backward),
	}
}

func ApplyVerdict(d *c37118.PMUData, v TimestampVerdict) bool {
	switch v.Action {
	case TimestampAccept:
		return true
	case TimestampClamp:
		d.Timestamp = v.CorrectedTS
		d.Stat |= 0x0800
		return true
	case TimestampReject:
		return false
	default:
		return true
	}
}
