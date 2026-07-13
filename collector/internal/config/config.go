// Package config loads the collector's runtime configuration from the environment
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

// Config holds the collector's runtime configuration.
type Config struct {
	BrokerAddr string
	Group      string
	Topic      string
	ConsumerID string
	DBDSN      string
	BatchSize  int
	LogLevel   string
	LogFormat  string
}

// Load reads configuration from the environment, applying defaults. CONSUMER_ID
// defaults to the pod hostname so each collector joins the group under a unique id.
func Load() Config {
	return Config{
		BrokerAddr: getenv("BROKER_ADDR", "message-queue:9000"),
		Group:      getenv("GROUP", "collectors"),
		Topic:      getenv("TOPIC", "telemetry"),
		ConsumerID: getenv("CONSUMER_ID", defaultConsumerID()),
		DBDSN:      getenv("DB_DSN", "postgres://telemetry:telemetry@database:5432/telemetry?sslmode=disable"),
		BatchSize:  getenvInt("BATCH_SIZE", 200),
		LogLevel:   getenv("LOG_LEVEL", "info"),
		LogFormat:  getenv("LOG_FORMAT", "json"),
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

func defaultConsumerID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "collector"
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
