package config

import (
	"flag"
	"os"
	"strconv"
	"time"
)

type Config struct {
	TCP          TCPConfig
	UDP          UDPConfig
	Database     DatabaseConfig
	Buffer       BufferConfig
	Metrics      MetricsConfig
	PMU          PMUConfig
}

type TCPConfig struct {
	Enabled       bool
	ListenAddr    string
	MaxConns      int
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
}

type UDPConfig struct {
	Enabled    bool
	ListenAddr string
	ReadBuffer int
}

type DatabaseConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	Database     string
	SSLMode      string
	MaxOpenConns int
	MaxIdleConns int
	BatchSize    int
	FlushInterval time.Duration
	WriteWorkers int
}

type BufferConfig struct {
	ChannelSize     int
	RingBufferSize  int
	ShardCount      int
	DropPolicy      string
}

type MetricsConfig struct {
	Enabled    bool
	ListenAddr string
}

type PMUConfig struct {
	ExpectedFrameRate int
	MaxPhasors        int
}

func Load() *Config {
	cfg := &Config{}

	flag.BoolVar(&cfg.TCP.Enabled, "tcp-enabled", envBool("TCP_ENABLED", true), "Enable TCP listener")
	flag.StringVar(&cfg.TCP.ListenAddr, "tcp-addr", envString("TCP_ADDR", ":4712"), "TCP listen address")
	flag.IntVar(&cfg.TCP.MaxConns, "tcp-max-conns", envInt("TCP_MAX_CONNS", 1024), "Max TCP connections")
	flag.DurationVar(&cfg.TCP.ReadTimeout, "tcp-read-timeout", envDuration("TCP_READ_TIMEOUT", 30*time.Second), "TCP read timeout")
	flag.DurationVar(&cfg.TCP.WriteTimeout, "tcp-write-timeout", envDuration("TCP_WRITE_TIMEOUT", 5*time.Second), "TCP write timeout")

	flag.BoolVar(&cfg.UDP.Enabled, "udp-enabled", envBool("UDP_ENABLED", true), "Enable UDP listener")
	flag.StringVar(&cfg.UDP.ListenAddr, "udp-addr", envString("UDP_ADDR", ":4712"), "UDP listen address")
	flag.IntVar(&cfg.UDP.ReadBuffer, "udp-read-buffer", envInt("UDP_READ_BUFFER", 16*1024*1024), "UDP socket read buffer size")

	flag.StringVar(&cfg.Database.Host, "db-host", envString("DB_HOST", "localhost"), "Database host")
	flag.IntVar(&cfg.Database.Port, "db-port", envInt("DB_PORT", 5432), "Database port")
	flag.StringVar(&cfg.Database.User, "db-user", envString("DB_USER", "postgres"), "Database user")
	flag.StringVar(&cfg.Database.Password, "db-password", envString("DB_PASSWORD", "postgres"), "Database password")
	flag.StringVar(&cfg.Database.Database, "db-name", envString("DB_NAME", "wams"), "Database name")
	flag.StringVar(&cfg.Database.SSLMode, "db-sslmode", envString("DB_SSLMODE", "disable"), "Database SSL mode")
	flag.IntVar(&cfg.Database.MaxOpenConns, "db-max-open", envInt("DB_MAX_OPEN", 64), "Max open DB connections")
	flag.IntVar(&cfg.Database.MaxIdleConns, "db-max-idle", envInt("DB_MAX_IDLE", 16), "Max idle DB connections")
	flag.IntVar(&cfg.Database.BatchSize, "db-batch-size", envInt("DB_BATCH_SIZE", 5000), "DB batch insert size")
	flag.DurationVar(&cfg.Database.FlushInterval, "db-flush-interval", envDuration("DB_FLUSH_INTERVAL", 100*time.Millisecond), "DB flush interval")
	flag.IntVar(&cfg.Database.WriteWorkers, "db-write-workers", envInt("DB_WRITE_WORKERS", 8), "DB write worker count")

	flag.IntVar(&cfg.Buffer.ChannelSize, "buf-channel-size", envInt("BUF_CHANNEL_SIZE", 65536), "Buffer channel size")
	flag.IntVar(&cfg.Buffer.RingBufferSize, "buf-ring-size", envInt("BUF_RING_SIZE", 131072), "Ring buffer size per shard")
	flag.IntVar(&cfg.Buffer.ShardCount, "buf-shard-count", envInt("BUF_SHARD_COUNT", 32), "Buffer shard count")
	flag.StringVar(&cfg.Buffer.DropPolicy, "buf-drop-policy", envString("BUF_DROP_POLICY", "oldest"), "Buffer drop policy: oldest/newest/block")

	flag.BoolVar(&cfg.Metrics.Enabled, "metrics-enabled", envBool("METRICS_ENABLED", true), "Enable metrics endpoint")
	flag.StringVar(&cfg.Metrics.ListenAddr, "metrics-addr", envString("METRICS_ADDR", ":9090"), "Metrics listen address")

	flag.IntVar(&cfg.PMU.ExpectedFrameRate, "pmu-frame-rate", envInt("PMU_FRAME_RATE", 100), "Expected PMU frame rate (Hz)")
	flag.IntVar(&cfg.PMU.MaxPhasors, "pmu-max-phasors", envInt("PMU_MAX_PHASORS", 32), "Max phasors per PMU")

	flag.Parse()
	return cfg
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
