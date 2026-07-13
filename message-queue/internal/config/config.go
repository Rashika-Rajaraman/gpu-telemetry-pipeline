// Package config loads the broker's runtime configuration from the environment and
// constructs its logger, keeping these concerns out of the thin main entrypoint.
// Log level and format are read from the environment so they can be tuned via the
// Kubernetes ConfigMap without a rebuild.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// Config holds the broker's runtime configuration.
type Config struct {
	ListenAddr   string
	Partitions   int
	BufferSize   int
	BatchSize    int
	PollInterval time.Duration
	LogLevel     string
	LogFormat    string
}

// Load reads configuration from the environment, applying defaults.
func Load() Config {
	return Config{
		ListenAddr:   getenv("LISTEN_ADDR", ":9000"),
		Partitions:   getenvInt("PARTITIONS", 16),
		BufferSize:   getenvInt("BUFFER_SIZE", 10000),
		BatchSize:    getenvInt("BATCH_SIZE", 100),
		PollInterval: time.Duration(getenvInt("POLL_INTERVAL_MS", 20)) * time.Millisecond,
		LogLevel:     getenv("LOG_LEVEL", "info"),
		LogFormat:    getenv("LOG_FORMAT", "json"),
	}
}

// NewLogger builds a logrus logger from the config. LOG_LEVEL accepts logrus levels
// (debug, info, warn, error); LOG_FORMAT accepts "json" (default) or "text". An
// unrecognized level falls back to info.
func NewLogger(cfg Config) *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)

	level, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	if strings.EqualFold(cfg.LogFormat, "text") {
		logger.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	} else {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}
	return logger
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
