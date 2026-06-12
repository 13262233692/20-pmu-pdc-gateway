package analytics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type AlertSeverity int

const (
	SeverityInfo AlertSeverity = iota
	SeverityWarning
	SeverityCritical
	SeverityEmergency
)

type AlertType int

const (
	AlertLowDamping AlertType = iota
	AlertNegativeDamping
	AlertFrequencyAbnormal
	AlertResonance
)

type OscillationAlert struct {
	ID            uint64
	Type          AlertType
	Severity      AlertSeverity
	Timestamp     uint64
	PMUID         uint16
	PMUName       string
	Frequency     float64
	DampingRatio  float64
	DampingFactor float64
	Amplitude     float64
	Duration      time.Duration
	Description   string
	Action        string
}

type DampingTracker struct {
	mu             sync.Mutex
	pmuID          uint16
	consecutiveLow int
	thresholdCount int
	lastDamping    float64
	lastFreq       float64
	startTime      uint64
	alertActive    bool
	lastAlertID    uint64
}

func NewDampingTracker(pmuID uint16, thresholdCount int) *DampingTracker {
	return &DampingTracker{
		pmuID:          pmuID,
		thresholdCount: thresholdCount,
	}
}

func (dt *DampingTracker) Update(damping, freq float64, ts uint64) (alert bool, severity AlertSeverity) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	dt.lastDamping = damping
	dt.lastFreq = freq

	warnThreshold := 0.05
	critThreshold := 0.02

	if damping >= 0 && damping < warnThreshold {
		dt.consecutiveLow++
		if dt.startTime == 0 {
			dt.startTime = ts
		}

		if dt.consecutiveLow >= dt.thresholdCount {
			if !dt.alertActive {
				dt.alertActive = true
				alert = true
				if damping < critThreshold {
					severity = SeverityCritical
				} else {
					severity = SeverityWarning
				}
			}
		}
	} else if damping < 0 {
		dt.consecutiveLow++
		if dt.startTime == 0 {
			dt.startTime = ts
		}

		if dt.consecutiveLow >= dt.thresholdCount/3 {
			if !dt.alertActive {
				dt.alertActive = true
				alert = true
				severity = SeverityEmergency
			}
		}
	} else {
		dt.consecutiveLow = 0
		dt.startTime = 0
		if dt.alertActive {
			dt.alertActive = false
		}
	}

	return
}

func (dt *DampingTracker) IsAlertActive() bool {
	dt.mu.Lock()
	active := dt.alertActive
	dt.mu.Unlock()
	return active
}

func (dt *DampingTracker) State() (damping float64, freq float64, consecutive int, active bool) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	return dt.lastDamping, dt.lastFreq, dt.consecutiveLow, dt.alertActive
}

type DetectorConfig struct {
	WindowSeconds    float64
	SampleRate       float64
	AnalysisInterval time.Duration
	LowDampingThresh float64
	WarningDuration  time.Duration
	CriticalDuration time.Duration
	MinFrequency     float64
	MaxFrequency     float64
	MaxPMUs          int
	WorkerCount      int
	MinAmplitude     float64
}

func DefaultDetectorConfig() DetectorConfig {
	return DetectorConfig{
		WindowSeconds:    5.0,
		SampleRate:       100.0,
		AnalysisInterval: 200 * time.Millisecond,
		LowDampingThresh: 0.05,
		WarningDuration:  3 * time.Second,
		CriticalDuration: 1 * time.Second,
		MinFrequency:     0.1,
		MaxFrequency:     2.5,
		MaxPMUs:          512,
		WorkerCount:      4,
		MinAmplitude:     0.001,
	}
}

type LowFreqOscillationDetector struct {
	cfg         DetectorConfig
	wm          *FreqWindowManager
	mpmCfg      MPMConfig

	trackers    map[uint16]*DampingTracker
	trackerMu   sync.RWMutex

	alerts      []OscillationAlert
	alertMu     sync.Mutex
	alertID     uint64
	alertCb     func(OscillationAlert)

	analysisCh  chan uint16
	workerWg    sync.WaitGroup
	stopCh      chan struct{}
	once        sync.Once

	stats       DetectorStats
}

type DetectorStats struct {
	Analyses      uint64
	AlertsIssued  uint64
	ModesDetected uint64
	Errors        uint64
	ActiveAlerts  int64
}

func NewLowFreqOscillationDetector(cfg DetectorConfig, wm *FreqWindowManager,
	alertCb func(OscillationAlert)) *LowFreqOscillationDetector {

	mpmCfg := DefaultMPMConfig(cfg.SampleRate)
	mpmCfg.MinFrequency = cfg.MinFrequency
	mpmCfg.MaxFrequency = cfg.MaxFrequency
	mpmCfg.MinAmplitude = cfg.MinAmplitude
	mpmCfg.WindowSize = int(cfg.WindowSeconds * cfg.SampleRate)
	mpmCfg.PencilParam = mpmCfg.WindowSize / 2

	det := &LowFreqOscillationDetector{
		cfg:       cfg,
		wm:        wm,
		mpmCfg:    mpmCfg,
		trackers:  make(map[uint16]*DampingTracker),
		alertCb:   alertCb,
		analysisCh: make(chan uint16, cfg.MaxPMUs),
		stopCh:    make(chan struct{}),
	}

	return det
}

func (d *LowFreqOscillationDetector) Start() {
	for i := 0; i < d.cfg.WorkerCount; i++ {
		d.workerWg.Add(1)
		go d.analysisWorker()
	}

	go d.schedulerLoop()
}

func (d *LowFreqOscillationDetector) Stop() {
	d.once.Do(func() {
		close(d.stopCh)
	})
	d.workerWg.Wait()
}

func (d *LowFreqOscillationDetector) schedulerLoop() {
	ticker := time.NewTicker(d.cfg.AnalysisInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.dispatchAnalyses()
		}
	}
}

func (d *LowFreqOscillationDetector) dispatchAnalyses() {
	windows := d.wm.AllWindows()
	requiredSamples := int(d.cfg.WindowSeconds * d.cfg.SampleRate * 0.8)

	for _, w := range windows {
		if w.Len() >= requiredSamples {
			select {
			case d.analysisCh <- w.pmuID:
			default:
			}
		}
	}
}

func (d *LowFreqOscillationDetector) analysisWorker() {
	defer d.workerWg.Done()

	for {
		select {
		case <-d.stopCh:
			return
		case pmuID := <-d.analysisCh:
			d.analyzePMU(pmuID)
		}
	}
}

func (d *LowFreqOscillationDetector) analyzePMU(pmuID uint16) {
	win, ok := d.wm.GetWindow(pmuID)
	if !ok {
		return
	}

	samples := win.FreqValues()
	if len(samples) < d.mpmCfg.WindowSize/2 {
		return
	}

	useSamples := samples
	if len(useSamples) > d.mpmCfg.WindowSize {
		useSamples = samples[len(samples)-d.mpmCfg.WindowSize:]
	}

	result := MatrixPencilMethod(useSamples, d.mpmCfg)
	atomic.AddUint64(&d.stats.Analyses, 1)

	if result == nil || len(result.Modes) == 0 {
		return
	}

	atomic.AddUint64(&d.stats.ModesDetected, uint64(len(result.Modes)))

	worstMode := FindLowestDampingMode(result, d.cfg.MinFrequency, d.cfg.MaxFrequency)
	if worstMode == nil {
		return
	}

	d.trackerMu.RLock()
	tracker, exists := d.trackers[pmuID]
	d.trackerMu.RUnlock()

	if !exists {
		thresholdCount := int(d.cfg.WarningDuration / d.cfg.AnalysisInterval)
		tracker = NewDampingTracker(pmuID, thresholdCount)
		d.trackerMu.Lock()
		d.trackers[pmuID] = tracker
		d.trackerMu.Unlock()
	}

	lastTS := uint64(time.Now().UnixNano())
	alert, severity := tracker.Update(worstMode.DampingRatio, worstMode.Frequency, lastTS)

	if alert {
		d.issueAlert(pmuID, worstMode, severity, lastTS)
	}
}

func (d *LowFreqOscillationDetector) issueAlert(pmuID uint16, mode *OscillationMode,
	severity AlertSeverity, ts uint64) {

	alertID := atomic.AddUint64(&d.alertID, 1)

	alert := OscillationAlert{
		ID:            alertID,
		Type:          AlertLowDamping,
		Severity:      severity,
		Timestamp:     ts,
		PMUID:         pmuID,
		Frequency:     mode.Frequency,
		DampingRatio:  mode.DampingRatio,
		DampingFactor: mode.DampingFactor,
		Amplitude:     mode.Amplitude,
		Description:   d.buildAlertDescription(pmuID, mode, severity),
		Action:        d.buildAlertAction(severity),
	}

	d.alertMu.Lock()
	d.alerts = append(d.alerts, alert)
	if len(d.alerts) > 10000 {
		d.alerts = d.alerts[len(d.alerts)-10000:]
	}
	d.alertMu.Unlock()

	atomic.AddUint64(&d.stats.AlertsIssued, 1)
	atomic.AddInt64(&d.stats.ActiveAlerts, 1)

	if d.alertCb != nil {
		d.alertCb(alert)
	}
}

func (d *LowFreqOscillationDetector) buildAlertDescription(pmuID uint16, mode *OscillationMode, severity AlertSeverity) string {
	switch severity {
	case SeverityEmergency:
		return "严重：检测到负阻尼低频振荡，系统存在共振失稳风险"
	case SeverityCritical:
		return "危急：500kV主干线路阻尼比低于安全红线，振荡持续放大"
	case SeverityWarning:
		return "预警：检测到弱阻尼低频振荡模式，需密切关注"
	default:
		return "信息：检测到低频振荡模式"
	}
}

func (d *LowFreqOscillationDetector) buildAlertAction(severity AlertSeverity) string {
	switch severity {
	case SeverityEmergency:
		return "主动熔断：立即启动全网主动防御，切断扰动源，启动负荷减载"
	case SeverityCritical:
		return "紧急：向主调度总控发起最高规格主动熔断拦截请求"
	case SeverityWarning:
		return "告警：通知调度运行人员，加强电网运行监视"
	default:
		return "记录：仅记录日志"
	}
}

func (d *LowFreqOscillationDetector) Stats() DetectorStats {
	active := atomic.LoadInt64(&d.stats.ActiveAlerts)
	return DetectorStats{
		Analyses:      atomic.LoadUint64(&d.stats.Analyses),
		AlertsIssued:  atomic.LoadUint64(&d.stats.AlertsIssued),
		ModesDetected: atomic.LoadUint64(&d.stats.ModesDetected),
		Errors:        atomic.LoadUint64(&d.stats.Errors),
		ActiveAlerts:  active,
	}
}

func (d *LowFreqOscillationDetector) RecentAlerts(n int) []OscillationAlert {
	d.alertMu.Lock()
	defer d.alertMu.Unlock()

	if n > len(d.alerts) {
		n = len(d.alerts)
	}
	if n <= 0 {
		return nil
	}

	out := make([]OscillationAlert, n)
	copy(out, d.alerts[len(d.alerts)-n:])
	return out
}

func (d *LowFreqOscillationDetector) GetDampingState(pmuID uint16) (float64, float64, int, bool) {
	d.trackerMu.RLock()
	tracker, ok := d.trackers[pmuID]
	d.trackerMu.RUnlock()

	if !ok {
		return 0, 0, 0, false
	}
	return tracker.State()
}

func (d *LowFreqOscillationDetector) HandlePMUData(data *c37118.PMUData) {
	if d.wm != nil {
		d.wm.HandlePMUData(data)
	}
}
