// Command collector consumes telemetry from the custom message queue as part of a
// consumer group, parses each DCGM CSV line into a sample, and persists it to
// PostgreSQL. Collectors scale horizontally: the broker rebalances partitions
// across the group as replicas join/leave.
package main

import (
	"context"
	"errors"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/gpu-telemetry-pipeline/collector/internal/config"
	"github.com/gpu-telemetry-pipeline/collector/internal/pipeline"
	"github.com/gpu-telemetry-pipeline/collector/internal/writer"
	"github.com/gpu-telemetry-pipeline/messagequeue/client"
)

// defaultRetryBackoff is the wait between dependency connection attempts.
const defaultRetryBackoff = 2 * time.Second

func main() {
	cfg := config.Load()
	logger := config.NewLogger(cfg)
	logger.WithFields(logrus.Fields{
		"broker":      cfg.BrokerAddr,
		"group":       cfg.Group,
		"topic":       cfg.Topic,
		"consumer_id": cfg.ConsumerID,
	}).Info("collector starting")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := connectDB(ctx, cfg.DBDSN, logger)
	if err != nil {
		logger.Info("shutting down before database connection established")
		return
	}
	defer store.Close()

	consumer, err := subscribeWithRetry(ctx, cfg, logger)
	if err != nil {
		logger.Info("shutting down before broker subscription established")
		return
	}
	defer consumer.Close()

	p := &pipeline.Pipeline{Consumer: consumer, Writer: store, Logger: logger}
	if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.WithError(err).Fatal("pipeline stopped with error")
	}
	logger.Info("collector shut down cleanly")
}

// connectDB connects to PostgreSQL, retrying with a fixed backoff because the
// database pod may start after the collector in Kubernetes.
func connectDB(ctx context.Context, dsn string, logger *logrus.Logger) (*writer.Postgres, error) {
	for attempt := 1; ; attempt++ {
		store, err := writer.NewPostgres(ctx, dsn)
		if err == nil {
			logger.Info("connected to database")
			return store, nil
		}
		logger.WithError(err).WithField("attempt", attempt).Warn("database not ready, retrying")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(defaultRetryBackoff):
		}
	}
}

// subscribeWithRetry joins the consumer group, retrying because the broker pod may
// start after the collector.
func subscribeWithRetry(ctx context.Context, cfg config.Config, logger *logrus.Logger) (*client.Consumer, error) {
	for attempt := 1; ; attempt++ {
		c, err := client.Subscribe(cfg.BrokerAddr, cfg.Group, cfg.Topic, cfg.ConsumerID)
		if err == nil {
			logger.WithField("addr", cfg.BrokerAddr).Info("subscribed to broker")
			return c, nil
		}
		logger.WithError(err).WithField("attempt", attempt).Warn("broker not ready, retrying")
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(defaultRetryBackoff):
		}
	}
}
