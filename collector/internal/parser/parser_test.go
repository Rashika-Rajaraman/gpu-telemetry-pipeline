package parser

import (
	"testing"
	"time"
)

func fixedParser() *Parser {
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &Parser{Now: func() time.Time { return fixed }}
}

const utilLine = `"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-bbb","NVIDIA H100 80GB HBM3","mtv5-dgx1-hgpu-031","","","","100","DCGM_FI_DRIVER_VERSION=""535.129.03"",gpu=""1"""`

func TestParseValidRow(t *testing.T) {
	p := fixedParser()
	s, err := p.Parse([]byte(utilLine))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Metric != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("metric = %q", s.Metric)
	}
	if s.Value != 100 {
		t.Errorf("value = %v, want 100", s.Value)
	}
	if s.UUID != "GPU-bbb" {
		t.Errorf("uuid = %q", s.UUID)
	}
	if s.GPUIndex != "1" || s.Device != "nvidia1" {
		t.Errorf("index/device = %q/%q", s.GPUIndex, s.Device)
	}
	if s.Hostname != "mtv5-dgx1-hgpu-031" {
		t.Errorf("hostname = %q", s.Hostname)
	}
	if !s.Timestamp.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("timestamp not assigned at parse time: %v", s.Timestamp)
	}
}

func TestParseFloatValue(t *testing.T) {
	line := `"t","DCGM_FI_DEV_POWER_USAGE","0","nvidia0","GPU-aaa","H100","host","","","","71.043","x"`
	s, err := fixedParser().Parse([]byte(line))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Value != 71.043 {
		t.Fatalf("value = %v, want 71.043", s.Value)
	}
}

func TestParseErrors(t *testing.T) {
	p := fixedParser()
	cases := []struct {
		name string
		line string
	}{
		{"too few fields", `"t","M","0","d","GPU-x"`},
		{"bad value", `"t","M","0","d","GPU-x","model","host","","","","not-a-number","x"`},
		{"empty uuid", `"t","M","0","d","","model","host","","","","10","x"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := p.Parse([]byte(tc.line)); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestNewParserAssignsCurrentTime(t *testing.T) {
	p := New()
	before := time.Now().Add(-time.Second)
	s, err := p.Parse([]byte(utilLine))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Timestamp.Before(before) {
		t.Fatalf("timestamp %v not assigned at parse time", s.Timestamp)
	}
}
