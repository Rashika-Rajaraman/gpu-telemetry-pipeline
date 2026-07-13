// Package pipeline wires the collector together: it consumes records from the
// message queue, parses each DCGM line into a sample, persists the batch, and then
// acknowledges it. Acking only after a successful write gives at-least-once
// semantics — a crash before the write means the broker redelivers.
package pipeline

import (
	"context"

	"github.com/sirupsen/logrus"

	"github.com/gpu-telemetry-pipeline/collector/internal/parser"
	"github.com/gpu-telemetry-pipeline/collector/internal/writer"
	"github.com/gpu-telemetry-pipeline/messagequeue/client"
)

// Consumer is the subset of the message-queue client the pipeline needs. It is
// satisfied by *client.Consumer and by fakes in tests.
type Consumer interface {
	Poll(ctx context.Context) ([]client.Record, error)
	AckRecords(recs []client.Record) error
}

// Pipeline consumes, parses, persists, and acknowledges telemetry.
type Pipeline struct {
	Consumer Consumer
	Writer   writer.Store
	Parser   *parser.Parser
	Logger   *logrus.Logger
}

// Run processes records until the consumer returns an error (e.g. ctx cancelled).
// Malformed records are skipped so one bad row cannot stall the stream.
func (p *Pipeline) Run(ctx context.Context) error {
	log := p.logger().WithField("component", "collector")
	parse := p.parser()
	log.Info("collector pipeline started")

	for {
		recs, err := p.Consumer.Poll(ctx)
		if err != nil {
			log.WithError(err).Info("pipeline stopping")
			return err
		}
		if len(recs) == 0 {
			continue
		}

		samples := parseRecords(parse, log, recs)
		if len(samples) > 0 {
			if err := p.Writer.Insert(ctx, samples); err != nil {
				log.WithError(err).Error("persist failed; leaving records unacked for redelivery")
				return err
			}
			log.WithField("count", len(samples)).Debug("persisted batch")
		}

		if err := p.Consumer.AckRecords(recs); err != nil {
			log.WithError(err).Error("acknowledge failed")
			return err
		}
	}
}

// parseRecords parses each record, skipping (and logging) malformed ones.
func parseRecords(parse *parser.Parser, log *logrus.Entry, recs []client.Record) []parser.Sample {
	samples := make([]parser.Sample, 0, len(recs))
	for _, r := range recs {
		s, err := parse.Parse(r.Value)
		if err != nil {
			log.WithError(err).Debug("skipping malformed record")
			continue
		}
		samples = append(samples, s)
	}
	return samples
}

func (p *Pipeline) logger() *logrus.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return logrus.StandardLogger()
}

func (p *Pipeline) parser() *parser.Parser {
	if p.Parser != nil {
		return p.Parser
	}
	return parser.New()
}
