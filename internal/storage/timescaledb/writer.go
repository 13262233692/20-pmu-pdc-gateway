package timescaledb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/wams/pmu-pdc-gateway/internal/config"
	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
)

type WriterStats struct {
	BatchesWritten uint64
	RowsWritten    uint64
	WriteErrors    uint64
	WriteLatencyNs uint64
}

type Writer struct {
	cfg    config.DatabaseConfig
	pool   *pgxpool.Pool
	pmuPool interface{}

	writeWg     sync.WaitGroup
	flushCh     chan []*c37118.PMUData
	stopCh      chan struct{}
	once        sync.Once

	stats       WriterStats
}

func NewWriter(cfg config.DatabaseConfig) *Writer {
	return &Writer{
		cfg:     cfg,
		flushCh: make(chan []*c37118.PMUData, cfg.WriteWorkers*4),
		stopCh:  make(chan struct{}),
	}
}

func (w *Writer) Connect(ctx context.Context) error {
	cs := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s pool_max_conns=%d pool_min_conns=%d",
		w.cfg.Host, w.cfg.Port, w.cfg.User, w.cfg.Password,
		w.cfg.Database, w.cfg.SSLMode, w.cfg.MaxOpenConns, w.cfg.MaxIdleConns,
	)

	poolCfg, err := pgxpool.ParseConfig(cs)
	if err != nil {
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}

	w.pool = pool
	return nil
}

func (w *Writer) Start() {
	for i := 0; i < w.cfg.WriteWorkers; i++ {
		w.writeWg.Add(1)
		go w.writeWorker()
	}
}

func (w *Writer) Stop() {
	w.once.Do(func() {
		close(w.stopCh)
	})
	w.writeWg.Wait()
	if w.pool != nil {
		w.pool.Close()
	}
}

func (w *Writer) SubmitBatch(batch []*c37118.PMUData) {
	select {
	case <-w.stopCh:
		return
	default:
	}

	select {
	case w.flushCh <- batch:
	default:
		select {
		case w.flushCh <- batch:
		case <-w.stopCh:
		}
	}
}

func (w *Writer) writeWorker() {
	defer w.writeWg.Done()

	for {
		select {
		case <-w.stopCh:
			w.drainRemaining()
			return
		case batch := <-w.flushCh:
			if len(batch) > 0 {
				w.writeBatch(batch)
			}
		}
	}
}

func (w *Writer) drainRemaining() {
	for {
		select {
		case batch := <-w.flushCh:
			if len(batch) > 0 {
				w.writeBatch(batch)
			}
		default:
			return
		}
	}
}

func (w *Writer) writeBatch(batch []*c37118.PMUData) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rows := make([][]interface{}, 0, len(batch))
	for _, d := range batch {
		if d == nil {
			continue
		}
		ts := time.Unix(0, d.UnixNano()).UTC()

		phasorReals := make([]float64, len(d.Phasors))
		phasorImags := make([]float64, len(d.Phasors))
		for i, p := range d.Phasors {
			phasorReals[i] = p.Real
			phasorImags[i] = p.Imag
		}

		analogs := make([]float64, len(d.Analogs))
		copy(analogs, d.Analogs)

		digitals := make([]int32, len(d.Digitals))
		for i, dv := range d.Digitals {
			digitals[i] = int32(dv)
		}

		rows = append(rows, []interface{}{
			ts,
			int32(d.IDCode),
			d.Freq,
			d.DFreq,
			int32(d.Stat),
			phasorReals,
			phasorImags,
			analogs,
			digitals,
		})
	}

	if len(rows) == 0 {
		return
	}

	cols := []string{
		"time", "pmu_id", "freq", "dfreq", "stat",
		"phasor_reals", "phasor_imags", "analogs", "digitals",
	}

	tableName := pgx.Identifier{"pmu_data"}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		atomic.AddUint64(&w.stats.WriteErrors, 1)
		return
	}
	defer conn.Release()

	n, err := conn.CopyFrom(
		ctx,
		tableName,
		cols,
		pgx.CopyFromRows(rows),
	)

	elapsed := time.Since(start)
	atomic.AddUint64(&w.stats.WriteLatencyNs, uint64(elapsed.Nanoseconds()))

	if err != nil {
		atomic.AddUint64(&w.stats.WriteErrors, 1)
		return
	}

	atomic.AddUint64(&w.stats.BatchesWritten, 1)
	atomic.AddUint64(&w.stats.RowsWritten, uint64(n))
}

func (w *Writer) Stats() WriterStats {
	return WriterStats{
		BatchesWritten: atomic.LoadUint64(&w.stats.BatchesWritten),
		RowsWritten:    atomic.LoadUint64(&w.stats.RowsWritten),
		WriteErrors:    atomic.LoadUint64(&w.stats.WriteErrors),
		WriteLatencyNs: atomic.LoadUint64(&w.stats.WriteLatencyNs),
	}
}

func (w *Writer) Ping(ctx context.Context) error {
	if w.pool == nil {
		return fmt.Errorf("not connected")
	}
	return w.pool.Ping(ctx)
}
