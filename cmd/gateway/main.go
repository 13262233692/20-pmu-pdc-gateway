package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wams/pmu-pdc-gateway/internal/buffer"
	"github.com/wams/pmu-pdc-gateway/internal/config"
	"github.com/wams/pmu-pdc-gateway/internal/metrics"
	"github.com/wams/pmu-pdc-gateway/internal/protocol/c37118"
	"github.com/wams/pmu-pdc-gateway/internal/storage/timescaledb"
	"github.com/wams/pmu-pdc-gateway/internal/transport"
	"github.com/wams/pmu-pdc-gateway/pkg/pool"
)

func main() {
	cfg := config.Load()

	parserCfg := c37118.ParserConfig{
		PhasorFormat: c37118.PhasorFormatInt16,
		FreqFormat:   c37118.FreqFormatInt16,
		AnalogFormat: c37118.AnalogFormatInt16,
		PhasorCount:  cfg.PMU.MaxPhasors,
		AnalogCount:  4,
		DigitalCount: 2,
	}
	parser := c37118.NewParser(parserCfg)

	pmuPool := pool.NewPMUDataPool(
		parserCfg.PhasorCount,
		parserCfg.AnalogCount,
		parserCfg.DigitalCount,
	)
	bytePool := pool.NewByteBufferPool(
		4096, 16384, 65536, 262144,
	)
	batchPool := pool.NewPMUDataBatchPool(cfg.Database.BatchSize)

	dropPolicy := buffer.DropOldest
	switch cfg.Buffer.DropPolicy {
	case "newest":
		dropPolicy = buffer.DropNewest
	case "block":
		dropPolicy = buffer.DropBlock
	}

	sb := buffer.NewShardedBuffer(
		cfg.Buffer.ShardCount,
		cfg.Buffer.RingBufferSize,
		dropPolicy,
	)

	var sortingWindow *buffer.SortingWindow
	var stopCh chan struct{}

	if cfg.SortingWindow.Enabled {
		stopCh = make(chan struct{})

		frameInterval := time.Second / time.Duration(cfg.PMU.ExpectedFrameRate)
		validator := buffer.NewTimestampValidator(
			cfg.SortingWindow.MaxForwardDrift,
			cfg.SortingWindow.MaxBackwardStep,
			frameInterval,
		)

		onEvict := func(d *c37118.PMUData) {
			sb.Push(d)
		}

		sortingWindow = buffer.NewSortingWindow(
			cfg.SortingWindow.MaxSlots,
			cfg.SortingWindow.TTL,
			cfg.SortingWindow.WindowWidth,
			validator,
			onEvict,
		)

		go sortingWindow.RunTTLScanner(cfg.SortingWindow.ScannerInterval, stopCh)

		log.Printf("SortingWindow enabled: ttl=%v width=%v max_slots=%d scanner=%v",
			cfg.SortingWindow.TTL, cfg.SortingWindow.WindowWidth,
			cfg.SortingWindow.MaxSlots, cfg.SortingWindow.ScannerInterval)
	}

	handler := func(d *c37118.PMUData) {
		if sortingWindow != nil {
			sortingWindow.Insert(d)
			return
		}
		sb.Push(d)
	}

	var tcpServer *transport.TCPServer
	var udpServer *transport.UDPServer

	if cfg.TCP.Enabled {
		tcpServer = transport.NewTCPServer(cfg.TCP, parser, handler, pmuPool, bytePool)
		if err := tcpServer.Start(); err != nil {
			log.Printf("TCP server start failed: %v", err)
		} else {
			log.Printf("TCP server listening on %s", cfg.TCP.ListenAddr)
		}
	}

	if cfg.UDP.Enabled {
		udpServer = transport.NewUDPServer(cfg.UDP, parser, handler, pmuPool, bytePool)
		if err := udpServer.Start(); err != nil {
			log.Printf("UDP server start failed: %v", err)
		} else {
			log.Printf("UDP server listening on %s", cfg.UDP.ListenAddr)
		}
	}

	writer := timescaledb.NewWriter(cfg.Database)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := writer.Connect(ctx); err != nil {
		log.Printf("Warning: DB connect failed (will retry in background): %v", err)
	} else {
		log.Printf("Connected to TimescaleDB at %s:%d/%s",
			cfg.Database.Host, cfg.Database.Port, cfg.Database.Database)
	}
	cancel()

	writer.Start()

	batcher := timescaledb.NewBatcher(cfg.Database, sb, writer, batchPool, pmuPool)
	batcher.Start()

	registry := metrics.NewRegistry()
	metricServer := metrics.NewServer(cfg.Metrics, registry)
	if err := metricServer.Start(); err != nil {
		log.Printf("Metrics server start failed: %v", err)
	} else if cfg.Metrics.Enabled {
		log.Printf("Metrics server listening on %s", cfg.Metrics.ListenAddr)
	}

	go statsReporter(tcpServer, udpServer, sb, writer, batcher, sortingWindow, registry)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("PMU-PDC Gateway started. Press Ctrl+C to stop.")
	<-sigCh
	log.Println("Shutting down...")

	if stopCh != nil {
		close(stopCh)
	}
	if tcpServer != nil {
		tcpServer.Stop()
	}
	if udpServer != nil {
		udpServer.Stop()
	}
	if sortingWindow != nil {
		sortingWindow.ForceExpireAll()
	}
	batcher.Stop()
	writer.Stop()
	metricServer.Stop()

	log.Println("Gateway stopped gracefully")
}

func statsReporter(tcpSrv *transport.TCPServer, udpSrv *transport.UDPServer,
	sb *buffer.ShardedBuffer, writer *timescaledb.Writer,
	batcher *timescaledb.Batcher,
	sw *buffer.SortingWindow,
	reg *metrics.Registry) {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var (
		prevFrames  uint64
		prevWritten uint64
	)

	bufFrames := reg.Counter("pmu_frames_parsed_total")
	bufDropped := reg.Counter("pmu_frames_dropped_total")
	dbWritten := reg.Counter("pmu_rows_written_total")
	dbErrors := reg.Counter("pmu_db_write_errors_total")
	activeConns := reg.Gauge("pmu_active_connections")
	bufLen := reg.Gauge("pmu_buffer_length")
	ppsGauge := reg.Gauge("pmu_pps")
	wpsGauge := reg.Gauge("pmu_writes_per_sec")
	swLen := reg.Gauge("pmu_sorting_window_slots")
	swExpired := reg.Counter("pmu_sorting_window_expired_total")
	swTTLBreach := reg.Counter("pmu_sorting_window_ttl_breaches_total")
	swBackwardDrops := reg.Counter("pmu_timestamp_backward_drops_total")

	for range ticker.C {
		var framesParsed, parseErrs uint64
		var activeConn uint64

		if tcpSrv != nil {
			st := tcpSrv.Stats()
			framesParsed += st.FramesParsed
			parseErrs += st.ParseErrors
			activeConn += st.ActiveConns
		}
		if udpSrv != nil {
			st := udpSrv.Stats()
			framesParsed += st.FramesParsed
			parseErrs += st.ParseErrors
		}

		bufStats := sb.Stats()
		dbStats := writer.Stats()
		batchStats := batcher.Stats()

		pps := framesParsed - prevFrames
		wps := dbStats.RowsWritten - prevWritten
		prevFrames = framesParsed
		prevWritten = dbStats.RowsWritten

		bufFrames.Add(framesParsed)
		bufDropped.Add(bufStats.Dropped)
		dbWritten.Add(dbStats.RowsWritten)
		dbErrors.Add(dbStats.WriteErrors)
		activeConns.SetInt(int64(activeConn))
		bufLen.SetInt(int64(sb.TotalLen()))
		ppsGauge.SetInt(int64(pps / 5))
		wpsGauge.SetInt(int64(wps / 5))

		var swStats buffer.WindowStats
		if sw != nil {
			swStats = sw.Stats()
			swLen.SetInt(swStats.CurrentSlots)
			swExpired.Add(swStats.Expired)
			swTTLBreach.Add(swStats.TTLBreaches)
			swBackwardDrops.Add(swStats.BackwardDrops)
		}

		log.Printf("stats: conns=%d buf=%d pps=%d wps=%d dropped=%d parse_err=%d db_err=%d batches=%d sw_slots=%d sw_expired=%d sw_ttl_breach=%d sw_backward=%d",
			activeConn, sb.TotalLen(), pps/5, wps/5,
			bufStats.Dropped, parseErrs, dbStats.WriteErrors, batchStats.BatchesCreated,
			swStats.CurrentSlots, swStats.Expired, swStats.TTLBreaches, swStats.BackwardDrops)
	}
}
