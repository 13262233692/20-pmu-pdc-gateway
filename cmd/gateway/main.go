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

	handler := func(d *c37118.PMUData) {
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

	go statsReporter(tcpServer, udpServer, sb, writer, batcher, registry)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("PMU-PDC Gateway started. Press Ctrl+C to stop.")
	<-sigCh
	log.Println("Shutting down...")

	if tcpServer != nil {
		tcpServer.Stop()
	}
	if udpServer != nil {
		udpServer.Stop()
	}
	batcher.Stop()
	writer.Stop()
	metricServer.Stop()

	log.Println("Gateway stopped gracefully")
}

func statsReporter(tcpSrv *transport.TCPServer, udpSrv *transport.UDPServer,
	sb *buffer.ShardedBuffer, writer *timescaledb.Writer,
	batcher *timescaledb.Batcher, reg *metrics.Registry) {

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var (
		prevFrames    uint64
		prevWritten   uint64
	)

	bufFrames := reg.Counter("pmu_frames_parsed_total")
	bufDropped := reg.Counter("pmu_frames_dropped_total")
	dbWritten := reg.Counter("pmu_rows_written_total")
	dbErrors := reg.Counter("pmu_db_write_errors_total")
	activeConns := reg.Gauge("pmu_active_connections")
	bufLen := reg.Gauge("pmu_buffer_length")
	ppsGauge := reg.Gauge("pmu_pps")
	wpsGauge := reg.Gauge("pmu_writes_per_sec")

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

		log.Printf("stats: conns=%d buf=%d pps=%d wps=%d dropped=%d parse_err=%d db_err=%d batches=%d",
			activeConn, sb.TotalLen(), pps/5, wps/5,
			bufStats.Dropped, parseErrs, dbStats.WriteErrors, batchStats.BatchesCreated)
	}
}
