package config

import (
	"testing"

	"github.com/sirupsen/logrus"
)

func unsetAll(t *testing.T) {
	t.Helper()
	for _, k := range []string{"LISTEN_ADDR", "DB_DSN", "LOG_LEVEL", "LOG_FORMAT"} {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	unsetAll(t)
	cfg := Load()

	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.DBDSN == "" {
		t.Error("DBDSN default should be non-empty")
	}
	if cfg.LogLevel != "info" || cfg.LogFormat != "json" {
		t.Errorf("log defaults = %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
}

func TestLoadOverrides(t *testing.T) {
	unsetAll(t)
	t.Setenv("LISTEN_ADDR", ":9999")
	t.Setenv("DB_DSN", "postgres://x")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")

	cfg := Load()
	if cfg.ListenAddr != ":9999" || cfg.DBDSN != "postgres://x" {
		t.Errorf("overrides not applied: %+v", cfg)
	}
	if cfg.LogLevel != "debug" || cfg.LogFormat != "text" {
		t.Errorf("log overrides not applied: %q/%q", cfg.LogLevel, cfg.LogFormat)
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
