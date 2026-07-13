package config

import (
	"testing"

	"github.com/sirupsen/logrus"
)

// unsetAll clears the streamer's env vars for the duration of a test.
func unsetAll(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"BROKER_ADDR", "CSV_PATH", "TOPIC", "ORDINAL", "POD_NAME",
		"REPLICAS", "INTERVAL_MS", "LOG_LEVEL", "LOG_FORMAT",
	} {
		t.Setenv(k, "") // getenv treats "" as unset; t.Setenv auto-restores
	}
}

func TestLoadDefaults(t *testing.T) {
	unsetAll(t)
	cfg := Load()

	if cfg.BrokerAddr != "messagequeue:9000" {
		t.Errorf("BrokerAddr = %q", cfg.BrokerAddr)
	}
	if cfg.CSVPath != "/data/dcgm_metrics_sample.csv" {
		t.Errorf("CSVPath = %q", cfg.CSVPath)
	}
	if cfg.Topic != "telemetry" {
		t.Errorf("Topic = %q", cfg.Topic)
	}
	if cfg.Ordinal != 0 {
		t.Errorf("Ordinal = %d", cfg.Ordinal)
	}
	if cfg.Replicas != 1 {
		t.Errorf("Replicas = %d", cfg.Replicas)
	}
	if cfg.IntervalMS != 100 {
		t.Errorf("IntervalMS = %d", cfg.IntervalMS)
	}
	if cfg.LogLevel != "info" || cfg.LogFormat != "json" {
		t.Errorf("log defaults = %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
}

func TestLoadOverrides(t *testing.T) {
	unsetAll(t)
	t.Setenv("BROKER_ADDR", "broker:1234")
	t.Setenv("CSV_PATH", "/x.csv")
	t.Setenv("TOPIC", "topic2")
	t.Setenv("REPLICAS", "5")
	t.Setenv("INTERVAL_MS", "250")
	t.Setenv("ORDINAL", "3")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")

	cfg := Load()
	if cfg.BrokerAddr != "broker:1234" || cfg.CSVPath != "/x.csv" || cfg.Topic != "topic2" {
		t.Errorf("string overrides not applied: %+v", cfg)
	}
	if cfg.Replicas != 5 || cfg.IntervalMS != 250 || cfg.Ordinal != 3 {
		t.Errorf("int overrides not applied: %+v", cfg)
	}
	if cfg.LogLevel != "debug" || cfg.LogFormat != "text" {
		t.Errorf("log overrides not applied: %q/%q", cfg.LogLevel, cfg.LogFormat)
	}
}

func TestOrdinal(t *testing.T) {
	t.Run("explicit ORDINAL wins", func(t *testing.T) {
		t.Setenv("ORDINAL", "4")
		t.Setenv("POD_NAME", "streamer-9")
		if got := ordinal(); got != 4 {
			t.Fatalf("ordinal = %d, want 4", got)
		}
	})
	t.Run("from POD_NAME suffix", func(t *testing.T) {
		t.Setenv("ORDINAL", "")
		t.Setenv("POD_NAME", "streamer-7")
		if got := ordinal(); got != 7 {
			t.Fatalf("ordinal = %d, want 7", got)
		}
	})
	t.Run("non-numeric POD_NAME defaults to 0", func(t *testing.T) {
		t.Setenv("ORDINAL", "")
		t.Setenv("POD_NAME", "streamer-abc")
		if got := ordinal(); got != 0 {
			t.Fatalf("ordinal = %d, want 0", got)
		}
	})
	t.Run("no hints defaults to 0", func(t *testing.T) {
		t.Setenv("ORDINAL", "")
		t.Setenv("POD_NAME", "")
		if got := ordinal(); got != 0 {
			t.Fatalf("ordinal = %d, want 0", got)
		}
	})
}

func TestGetenvInt(t *testing.T) {
	t.Setenv("N", "42")
	if got := getenvInt("N", 1); got != 42 {
		t.Errorf("valid = %d, want 42", got)
	}
	t.Setenv("N", "not-a-number")
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
