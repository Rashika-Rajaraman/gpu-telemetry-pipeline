package config

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func unsetAll(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"BROKER_ADDR", "GROUP", "TOPIC", "CONSUMER_ID", "DB_DSN",
		"BATCH_SIZE", "LOG_LEVEL", "LOG_FORMAT",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	unsetAll(t)
	cfg := Load()

	if cfg.BrokerAddr != "messagequeue:9000" {
		t.Errorf("BrokerAddr = %q", cfg.BrokerAddr)
	}
	if cfg.Group != "collectors" {
		t.Errorf("Group = %q", cfg.Group)
	}
	if cfg.Topic != "telemetry" {
		t.Errorf("Topic = %q", cfg.Topic)
	}
	if cfg.ConsumerID == "" {
		t.Error("ConsumerID default should be non-empty (hostname)")
	}
	if cfg.BatchSize != 200 {
		t.Errorf("BatchSize = %d", cfg.BatchSize)
	}
	if cfg.LogLevel != "info" || cfg.LogFormat != "json" {
		t.Errorf("log defaults = %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
}

func TestLoadOverrides(t *testing.T) {
	unsetAll(t)
	t.Setenv("BROKER_ADDR", "b:1")
	t.Setenv("GROUP", "g2")
	t.Setenv("TOPIC", "t2")
	t.Setenv("CONSUMER_ID", "c-42")
	t.Setenv("DB_DSN", "postgres://x")
	t.Setenv("BATCH_SIZE", "500")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")

	cfg := Load()
	if cfg.BrokerAddr != "b:1" || cfg.Group != "g2" || cfg.Topic != "t2" || cfg.ConsumerID != "c-42" {
		t.Errorf("string overrides not applied: %+v", cfg)
	}
	if cfg.DBDSN != "postgres://x" || cfg.BatchSize != 500 {
		t.Errorf("dsn/batch overrides not applied: %+v", cfg)
	}
	if cfg.LogLevel != "debug" || cfg.LogFormat != "text" {
		t.Errorf("log overrides not applied: %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
}

func TestGetenvInt(t *testing.T) {
	t.Setenv("N", "42")
	if got := getenvInt("N", 1); got != 42 {
		t.Errorf("valid = %d, want 42", got)
	}
	t.Setenv("N", "bad")
	if got := getenvInt("N", 7); got != 7 {
		t.Errorf("invalid should fall back to default, got %d", got)
	}
	t.Setenv("MISSING", "")
	if got := getenvInt("MISSING", 9); got != 9 {
		t.Errorf("missing should use default, got %d", got)
	}
}

func TestDefaultConsumerID(t *testing.T) {
	if id := defaultConsumerID(); id == "" {
		t.Fatal("defaultConsumerID should never be empty")
	}
}

func TestNewLogger(t *testing.T) {
	l := NewLogger(Config{LogLevel: "debug", LogFormat: "text"})
	if l.GetLevel() != logrus.DebugLevel {
		t.Errorf("level = %v, want debug", l.GetLevel())
	}
	if _, ok := l.Formatter.(*logrus.TextFormatter); !ok {
		t.Errorf("formatter = %T, want TextFormatter", l.Formatter)
	}

	l2 := NewLogger(Config{LogLevel: "bogus", LogFormat: "json"})
	if l2.GetLevel() != logrus.InfoLevel {
		t.Errorf("invalid level should fall back to info, got %v", l2.GetLevel())
	}
	if _, ok := l2.Formatter.(*logrus.JSONFormatter); !ok {
		t.Errorf("formatter = %T, want JSONFormatter", l2.Formatter)
	}
}
