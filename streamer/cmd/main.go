// Command streamer reads DCGM GPU telemetry from a CSV file and publishes each
// row to the custom message queue, keyed by GPU uuid so that all telemetry for a
// given GPU lands on the same partition (preserving per-GPU ordering).
//
// Horizontal scaling: each streamer replica is assigned an ordinal (0..N-1) and
// publishes only the CSV rows where rowIndex % N == ordinal, so scaling replicas
// up/down redistributes the stream without duplication.
package main

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/cisco-interview/telemetry-pipeline/messagequeue/client"
	"github.com/cisco-interview/telemetry-pipeline/streamer/internal/config"
	"github.com/cisco-interview/telemetry-pipeline/streamer/internal/publisher"
	"github.com/cisco-interview/telemetry-pipeline/streamer/internal/source"
)

const defaultDialBackoff = 2 * time.Second

func main() {
	cfg := config.Load()
	logger := config.NewLogger(cfg)
	logger.WithFields(logrus.Fields{
		"broker":      cfg.BrokerAddr,
		"csv":         cfg.CSVPath,
		"topic":       cfg.Topic,
		"ordinal":     cfg.Ordinal,
		"replicas":    cfg.Replicas,
		"interval_ms": cfg.IntervalMS,
	}).Info("streamer starting")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	src, err := source.Open(cfg.CSVPath, true, logger)
	if err != nil {
		logger.WithError(err).Fatal("failed to open telemetry source")
	}
	defer src.Close()

	prod, err := dialWithRetry(ctx, cfg.BrokerAddr, defaultDialBackoff, logger)
	if err != nil {
		// dialWithRetry returns only when the context is cancelled, i.e. a clean
		// shutdown before the broker became reachable.
		logger.Info("shutting down before broker connection established")
		return
	}
	defer prod.Close()

	pub := &publisher.Publisher{
		Producer: prod,
		Reader:   src,
		Topic:    cfg.Topic,
		Ordinal:  cfg.Ordinal,
		Replicas: cfg.Replicas,
		Interval: time.Duration(cfg.IntervalMS) * time.Millisecond,
		Logger:   logger,
	}

	if err := pub.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.WithError(err).Fatal("publisher stopped with error")
	}
	logger.Info("streamer shut down cleanly")
}

// dialWithRetry connects to the broker, retrying with a fixed backoff because the
// broker pod may start after the streamer in Kubernetes. It stops when the context
// is cancelled.
func dialWithRetry(ctx context.Context, addr string, backoff time.Duration, logger *logrus.Logger) (*client.Producer, error) {
	for attempt := 1; ; attempt++ {
		prod, err := client.Dial(addr)
		if err == nil {
			logger.WithField("addr", addr).Info("connected to broker")
			return prod, nil
		}
		logger.WithFields(logrus.Fields{"addr": addr, "attempt": attempt}).
			WithError(err).Warn("broker not ready, retrying")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
}
