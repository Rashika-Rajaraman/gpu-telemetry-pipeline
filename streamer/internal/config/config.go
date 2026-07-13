// Package config loads the streamer's runtime configuration from the environment
// and constructs its logger, keeping these concerns out of the thin main
// entrypoint. Log level and format are read from the environment so they can be
// tuned via the Kubernetes ConfigMap without a rebuild.
package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// Config holds the streamer's runtime configuration.
type Config struct {
	BrokerAddr string
	CSVPath    string
	Topic      string
	Ordinal    int
	Replicas   int
	IntervalMS int
	LogLevel   string
	LogFormat  string
}

// Load reads configuration from the environment, applying defaults. The replica
// ordinal is taken from ORDINAL, falling back to the numeric suffix of POD_NAME
// (as set for StatefulSet pods, e.g. "streamer-2" -> 2).
func Load() Config {
	return Config{
		BrokerAddr: getenv("BROKER_ADDR", "message-queue:9000"),
		CSVPath:    getenv("CSV_PATH", "/data/dcgm_metrics_sample.csv"),
		Topic:      getenv("TOPIC", "telemetry"),
		Ordinal:    ordinal(),
		Replicas:   getenvInt("REPLICAS", 1),
		IntervalMS: getenvInt("INTERVAL_MS", 100),
		LogLevel:   getenv("LOG_LEVEL", "info"),
		LogFormat:  getenv("LOG_FORMAT", "json"),
	}
}

// NewLogger builds a logrus logger from the config. LOG_LEVEL accepts logrus
// levels (debug, info, warn, error); LOG_FORMAT accepts "json" (default) or
// "text". An unrecognized level falls back to info.
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

// ordinal resolves this replica's index from ORDINAL or the POD_NAME suffix.
func ordinal() int {
	if v := os.Getenv("ORDINAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	if pod := os.Getenv("POD_NAME"); pod != "" {
		if i := strings.LastIndex(pod, "-"); i >= 0 {
			if n, err := strconv.Atoi(pod[i+1:]); err == nil {
				return n
			}
		}
	}
	return 0
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
