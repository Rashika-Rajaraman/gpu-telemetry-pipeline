// Package publisher owns the streamer's publish loop: it applies row sharding
// (rowIndex % replicas == ordinal) so that scaling streamer replicas splits the
// stream with no duplication, and publishes each selected row to the message queue
// keyed by GPU uuid, pacing output to simulate a live telemetry feed.
package publisher

import (
	"context"
	"io"
	"time"

	"github.com/sirupsen/logrus"
)

// progressEvery controls how often (in published rows) a progress line is logged.
const progressEvery = 1000

// Producer is the subset of the message-queue client the publisher needs. It is
// satisfied by *client.Producer and by fakes in tests.
type Producer interface {
	Publish(topic string, key, value []byte) (partition int, offset int64, err error)
}

// RowReader yields raw telemetry lines and their partition keys. Satisfied by
// *source.Source and by fakes in tests.
type RowReader interface {
	Next() (line, key []byte, err error)
}

// Publisher reads rows and publishes this replica's shard to the broker.
type Publisher struct {
	Producer Producer
	Reader   RowReader
	Topic    string
	Ordinal  int
	Replicas int
	Interval time.Duration
	Logger   *logrus.Logger
}

// shouldPublish reports whether this replica owns the given global row index.
func (p *Publisher) shouldPublish(idx int) bool {
	if p.Replicas <= 1 {
		return true
	}
	return idx%p.Replicas == p.Ordinal
}

// Run reads rows until the reader is exhausted (io.EOF) or ctx is cancelled,
// publishing this replica's shard. It returns nil on a clean EOF and ctx.Err() on
// cancellation.
func (p *Publisher) Run(ctx context.Context) error {
	log := p.logger().WithFields(logrus.Fields{
		"component": "publisher",
		"topic":     p.Topic,
		"ordinal":   p.Ordinal,
		"replicas":  p.Replicas,
	})
	log.Info("publisher started")

	var published, skipped int
	for idx := 0; ; idx++ {
		if err := ctx.Err(); err != nil {
			log.WithError(err).Info("publisher stopping")
			return err
		}

		line, key, err := p.Reader.Next()
		if err == io.EOF {
			log.WithFields(logrus.Fields{"published": published, "skipped": skipped}).
				Info("stream exhausted")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("reading row failed")
			return err
		}

		if !p.shouldPublish(idx) {
			skipped++
			continue
		}
		if err := p.publish(log, key, line); err != nil {
			return err
		}
		published++
		if published%progressEvery == 0 {
			log.WithField("published", published).Info("publish progress")
		}

		if err := p.pace(ctx); err != nil {
			return err
		}
	}
}

// publish sends one row to the broker.
func (p *Publisher) publish(log *logrus.Entry, key, line []byte) error {
	partition, offset, err := p.Producer.Publish(p.Topic, key, line)
	if err != nil {
		log.WithError(err).Error("publish failed")
		return err
	}
	log.WithFields(logrus.Fields{
		"partition": partition,
		"offset":    offset,
		"key":       string(key),
	}).Debug("published row")
	return nil
}

// pace waits the configured interval between publishes, if any, returning early if
// ctx is cancelled.
func (p *Publisher) pace(ctx context.Context) error {
	if p.Interval <= 0 {
		return nil
	}
	return sleep(ctx, p.Interval)
}

// logger returns the configured logger or the logrus standard logger.
func (p *Publisher) logger() *logrus.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return logrus.StandardLogger()
}

// sleep waits for d or until ctx is cancelled.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

