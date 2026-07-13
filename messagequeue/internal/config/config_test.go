package config

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func unsetAll(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"LISTEN_ADDR", "PARTITIONS", "BUFFER_SIZE", "BATCH_SIZE",
		"POLL_INTERVAL_MS", "LOG_LEVEL", "LOG_FORMAT",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	unsetAll(t)
	cfg := Load()

	if cfg.ListenAddr != ":9000" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.Partitions != 16 {
		t.Errorf("Partitions = %d", cfg.Partitions)
	}
	if cfg.BufferSize != 10000 {
		t.Errorf("BufferSize = %d", cfg.BufferSize)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("BatchSize = %d", cfg.BatchSize)
	}
	if cfg.PollInterval != 20*time.Millisecond {
		t.Errorf("PollInterval = %v", cfg.PollInterval)
	}
	if cfg.LogLevel != "info" || cfg.LogFormat != "json" {
		t.Errorf("log defaults = %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
}

func TestLoadOverrides(t *testing.T) {
	unsetAll(t)
	t.Setenv("LISTEN_ADDR", ":7000")
	t.Setenv("PARTITIONS", "8")
	t.Setenv("BUFFER_SIZE", "500")
	t.Setenv("BATCH_SIZE", "50")
	t.Setenv("POLL_INTERVAL_MS", "5")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")

	cfg := Load()
	if cfg.ListenAddr != ":7000" || cfg.Partitions != 8 || cfg.BufferSize != 500 || cfg.BatchSize != 50 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
	if cfg.PollInterval != 5*time.Millisecond {
		t.Errorf("PollInterval = %v, want 5ms", cfg.PollInterval)
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
