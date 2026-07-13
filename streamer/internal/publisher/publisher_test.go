package publisher

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// fakeReader yields a fixed set of rows then io.EOF.
type fakeReader struct {
	rows [][2]string // {line, key}
	i    int
}

func (f *fakeReader) Next() (line, key []byte, err error) {
	if f.i >= len(f.rows) {
		return nil, nil, io.EOF
	}
	r := f.rows[f.i]
	f.i++
	return []byte(r[0]), []byte(r[1]), nil
}

// fakeProducer records everything published.
type fakeProducer struct {
	mu        sync.Mutex
	published [][2]string // {key, value}
}

func (p *fakeProducer) Publish(topic string, key, value []byte) (int, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.published = append(p.published, [2]string{string(key), string(value)})
	return 0, int64(len(p.published)), nil
}

func makeRows(n int) [][2]string {
	rows := make([][2]string, n)
	for i := 0; i < n; i++ {
		rows[i] = [2]string{fmt.Sprintf("line-%d", i), fmt.Sprintf("GPU-%d", i)}
	}
	return rows
}

func TestShardingPublishesOnlyOwnedRows(t *testing.T) {
	const total = 10
	// 3 replicas: each should publish rows where idx % 3 == ordinal.
	for ordinal := 0; ordinal < 3; ordinal++ {
		prod := &fakeProducer{}
		pub := &Publisher{
			Producer: prod,
			Reader:   &fakeReader{rows: makeRows(total)},
			Topic:    "telemetry",
			Ordinal:  ordinal,
			Replicas: 3,
		}
		if err := pub.Run(context.Background()); err != nil {
			t.Fatalf("ordinal %d run: %v", ordinal, err)
		}
		var wantVals []string
		for i := 0; i < total; i++ {
			if i%3 == ordinal {
				wantVals = append(wantVals, fmt.Sprintf("line-%d", i))
			}
		}
		if len(prod.published) != len(wantVals) {
			t.Fatalf("ordinal %d published %d, want %d", ordinal, len(prod.published), len(wantVals))
		}
		for j, want := range wantVals {
			if prod.published[j][1] != want {
				t.Fatalf("ordinal %d row %d = %q, want %q", ordinal, j, prod.published[j][1], want)
			}
		}
	}
}

func TestShardingUnionCoversAllRowsOnce(t *testing.T) {
	const total = 20
	const replicas = 4
	seen := map[string]int{}
	for ordinal := 0; ordinal < replicas; ordinal++ {
		prod := &fakeProducer{}
		pub := &Publisher{
			Producer: prod,
			Reader:   &fakeReader{rows: makeRows(total)},
			Ordinal:  ordinal,
			Replicas: replicas,
		}
		if err := pub.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, kv := range prod.published {
			seen[kv[1]]++
		}
	}
	if len(seen) != total {
		t.Fatalf("union covered %d rows, want %d", len(seen), total)
	}
	for v, c := range seen {
		if c != 1 {
			t.Fatalf("row %q published %d times, want exactly 1", v, c)
		}
	}
}

func TestSingleReplicaPublishesEverything(t *testing.T) {
	prod := &fakeProducer{}
	pub := &Publisher{
		Producer: prod,
		Reader:   &fakeReader{rows: makeRows(5)},
		Replicas: 1,
	}
	if err := pub.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.published) != 5 {
		t.Fatalf("published %d, want 5", len(prod.published))
	}
	// Verify uuid keys were forwarded.
	if prod.published[0][0] != "GPU-0" {
		t.Fatalf("key = %q, want GPU-0", prod.published[0][0])
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	prod := &fakeProducer{}
	// Reader with effectively unlimited rows.
	pub := &Publisher{
		Producer: prod,
		Reader:   &endlessReader{},
		Replicas: 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	if err := pub.Run(ctx); err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

type endlessReader struct{ n int }

func (e *endlessReader) Next() (line, key []byte, err error) {
	e.n++
	return []byte("x"), []byte("k"), nil
}

// testLogger returns a discarding logger that exercises debug log paths.
func testLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	l.SetLevel(logrus.DebugLevel)
	return l
}

// errProducer always fails publishing.
type errProducer struct{ err error }

func (e *errProducer) Publish(topic string, key, value []byte) (int, int64, error) {
	return 0, 0, e.err
}

func TestPublishErrorStopsRun(t *testing.T) {
	wantErr := errors.New("broker down")
	pub := &Publisher{
		Producer: &errProducer{err: wantErr},
		Reader:   &fakeReader{rows: makeRows(3)},
		Replicas: 1,
		Logger:   testLogger(),
	}
	if err := pub.Run(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

// errReader returns a non-EOF error.
type errReader struct{ err error }

func (e *errReader) Next() (line, key []byte, err error) {
	return nil, nil, e.err
}

func TestReadErrorStopsRun(t *testing.T) {
	wantErr := errors.New("read fail")
	pub := &Publisher{
		Producer: &fakeProducer{},
		Reader:   &errReader{err: wantErr},
		Replicas: 1,
		Logger:   testLogger(),
	}
	if err := pub.Run(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
}

func TestIntervalPacing(t *testing.T) {
	prod := &fakeProducer{}
	pub := &Publisher{
		Producer: prod,
		Reader:   &fakeReader{rows: makeRows(3)},
		Replicas: 1,
		Interval: 2 * time.Millisecond,
		Logger:   testLogger(),
	}
	start := time.Now()
	if err := pub.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.published) != 3 {
		t.Fatalf("published %d, want 3", len(prod.published))
	}
	if time.Since(start) < 2*time.Millisecond {
		t.Fatalf("expected pacing delay, took %v", time.Since(start))
	}
}

func TestPaceStopsOnCancel(t *testing.T) {
	// With a long interval, a cancelled context aborts during pacing.
	pub := &Publisher{
		Producer: &fakeProducer{},
		Reader:   &endlessReader{},
		Replicas: 1,
		Interval: time.Hour,
		Logger:   testLogger(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := pub.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
}
