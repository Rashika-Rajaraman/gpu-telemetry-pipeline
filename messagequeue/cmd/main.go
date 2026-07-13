// Command broker runs the custom message queue: a TCP server implementing
// topics, partitions, consumer groups with rebalancing, at-least-once delivery
// with acks/redelivery, and producer backpressure via bounded partition buffers.
package main

import (
	"context"
	"net"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"

	"github.com/gpu-telemetry-pipeline/messagequeue/internal/broker"
	"github.com/gpu-telemetry-pipeline/messagequeue/internal/config"
)

func main() {
	cfg := config.Load()
	logger := config.NewLogger(cfg)

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		logger.WithError(err).WithField("addr", cfg.ListenAddr).Fatal("failed to listen")
	}
	logger.WithFields(logrus.Fields{
		"addr":       cfg.ListenAddr,
		"partitions": cfg.Partitions,
		"buffer":     cfg.BufferSize,
	}).Info("broker listening")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	b := broker.New(broker.Config{
		Partitions:   cfg.Partitions,
		BufferSize:   cfg.BufferSize,
		BatchSize:    cfg.BatchSize,
		PollInterval: cfg.PollInterval,
		Logger:       logger,
	})
	if err := b.Serve(ctx, ln); err != nil {
		logger.WithError(err).Fatal("serve error")
	}
	logger.Info("broker shut down cleanly")
}
