package source

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

const sampleCSV = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-aaa","NVIDIA H100","host-1","","","","10","x=""1,2"""
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-bbb","NVIDIA H100","host-1","","","","20","x=""3"""

"2025-07-18T20:42:34Z","DCGM_FI_DEV_POWER_USAGE","0","nvidia0","GPU-aaa","NVIDIA H100","host-1","","","","71.5","x=""4"""
`

func testLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	l.SetLevel(logrus.DebugLevel) // exercise debug log paths
	return l
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "dcgm.csv")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	return path
}

func TestSourceReadsRowsAndExtractsUUID(t *testing.T) {
	path := writeTemp(t, sampleCSV)
	s, err := Open(path, false, testLogger())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	wantKeys := []string{"GPU-aaa", "GPU-bbb", "GPU-aaa"}
	for i, want := range wantKeys {
		line, key, err := s.Next()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		if string(key) != want {
			t.Fatalf("row %d key = %q, want %q", i, key, want)
		}
		if len(line) == 0 {
			t.Fatalf("row %d empty line", i)
		}
	}
	// After the last data row, non-looping source returns EOF (blank line skipped).
	if _, _, err := s.Next(); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestSourceLoops(t *testing.T) {
	path := writeTemp(t, sampleCSV)
	s, err := Open(path, true, testLogger())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// Read more rows than the file has; looping should keep producing.
	seen := 0
	for i := 0; i < 7; i++ {
		_, key, err := s.Next()
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if len(key) > 0 {
			seen++
		}
	}
	if seen != 7 {
		t.Fatalf("looped reads = %d, want 7", seen)
	}
}

func TestUUIDColumnDetectedByHeaderName(t *testing.T) {
	// uuid placed in a non-canonical position; header lookup must still find it.
	csv := "a,b,uuid,value\n1,2,GPU-zzz,9\n"
	path := writeTemp(t, csv)
	s, err := Open(path, false, testLogger())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	_, key, err := s.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if string(key) != "GPU-zzz" {
		t.Fatalf("key = %q, want GPU-zzz", key)
	}
}

func TestUUIDFallsBackToCanonicalColumn(t *testing.T) {
	// Header has no "uuid" column, so the canonical index (4) is used.
	csv := "c0,c1,c2,c3,c4\nv0,v1,v2,v3,GPU-fallback\n"
	path := writeTemp(t, csv)
	s, err := Open(path, false, testLogger())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	_, key, err := s.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if string(key) != "GPU-fallback" {
		t.Fatalf("key = %q, want GPU-fallback", key)
	}
}

func TestOpenMissingFile(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "nope.csv"), false, testLogger()); err == nil {
		t.Fatal("expected error opening missing file")
	}
}

func TestOpenEmptyFile(t *testing.T) {
	path := writeTemp(t, "")
	if _, err := Open(path, false, testLogger()); err == nil {
		t.Fatal("expected error opening empty file")
	}
}

func TestOpenNilLoggerUsesStandard(t *testing.T) {
	path := writeTemp(t, sampleCSV)
	s, err := Open(path, false, nil) // nil logger must be tolerated
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if _, _, err := s.Next(); err != nil {
		t.Fatalf("next: %v", err)
	}
}

func TestCloseIsIdempotentBeforeOpen(t *testing.T) {
	// A source whose file handle is nil (never assigned) closes without error.
	s := &Source{}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestNextReturnsScanError(t *testing.T) {
	// A data line longer than the scanner's max buffer triggers a scan error.
	var b strings.Builder
	b.WriteString("uuid,value\n")
	b.Write(bytes.Repeat([]byte("a"), maxLineBytes+1024))
	b.WriteString("\n")

	path := writeTemp(t, b.String())
	s, err := Open(path, false, testLogger())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	if _, _, err := s.Next(); err == nil {
		t.Fatal("expected a scan error for an over-long line")
	}
}
