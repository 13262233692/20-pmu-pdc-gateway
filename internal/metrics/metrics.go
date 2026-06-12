package metrics

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/config"
)

type Counter struct {
	val uint64
}

func (c *Counter) Inc() {
	atomic.AddUint64(&c.val, 1)
}

func (c *Counter) Add(delta uint64) {
	atomic.AddUint64(&c.val, delta)
}

func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.val)
}

type Gauge struct {
	val uint64
}

func (g *Gauge) Set(v float64) {
	bits := math.Float64bits(v)
	atomic.StoreUint64(&g.val, bits)
}

func (g *Gauge) SetInt(v int64) {
	atomic.StoreUint64(&g.val, uint64(v))
}

func (g *Gauge) Value() float64 {
	return math.Float64frombits(atomic.LoadUint64(&g.val))
}

func (g *Gauge) ValueInt() int64 {
	return int64(atomic.LoadUint64(&g.val))
}

type Registry struct {
	counters map[string]*Counter
	gauges   map[string]*Gauge
	mu       sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{
		counters: make(map[string]*Counter),
		gauges:   make(map[string]*Gauge),
	}
}

func (r *Registry) Counter(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{}
	r.counters[name] = c
	return c
}

func (r *Registry) Gauge(name string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := &Gauge{}
	r.gauges[name] = g
	return g
}

type Server struct {
	cfg      config.MetricsConfig
	registry *Registry
	server   *http.Server
	once     sync.Once
}

func NewServer(cfg config.MetricsConfig, registry *Registry) *Server {
	return &Server{
		cfg:      cfg,
		registry: registry,
	}
}

func (s *Server) Start() error {
	if !s.cfg.Enabled {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealth)

	s.server = &http.Server{
		Addr:         s.cfg.ListenAddr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		}
	}()

	return nil
}

func (s *Server) Stop() {
	s.once.Do(func() {
		if s.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s.server.Shutdown(ctx)
		}
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	s.registry.mu.RLock()
	defer s.registry.mu.RUnlock()

	for name, c := range s.registry.counters {
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", name, name, c.Value())
	}
	for name, g := range s.registry.gauges {
		fmt.Fprintf(w, "# TYPE %s gauge\n%s %d\n", name, name, g.ValueInt())
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}
