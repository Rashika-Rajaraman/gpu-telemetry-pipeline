// Package parser converts a raw DCGM CSV line (the message payload produced by the
// streamer) into a structured telemetry sample for persistence. DCGM data is
// long-format: one metric per row. The GPU identity is the uuid column.
//
// The column layout follows the standard DCGM export; the shared contract between
// streamer and collector is the CSV format itself, so no Go types are shared.
package parser

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Canonical DCGM column positions:
//
//	timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
const (
	colMetric   = 1
	colGPUID    = 2
	colDevice   = 3
	colUUID     = 4
	colModel    = 5
	colHostname = 6
	colValue    = 10
	minFields   = 11
)

// Sample is a parsed telemetry reading ready for persistence.
type Sample struct {
	Timestamp time.Time
	Metric    string
	Value     float64
	UUID      string
	GPUIndex  string
	Device    string
	ModelName string
	Hostname  string
}

// Parser converts CSV lines into samples. Now supplies the collection timestamp
// (assigned at parse time per the spec) and is injectable for tests.
type Parser struct {
	Now func() time.Time
}

// New returns a Parser that timestamps samples with the current UTC time.
func New() *Parser {
	return &Parser{Now: func() time.Time { return time.Now().UTC() }}
}

// Parse decodes one DCGM CSV line into a Sample. It returns an error for
// malformed rows (too few fields or an unparseable value).
func (p *Parser) Parse(line []byte) (Sample, error) {
	r := csv.NewReader(bytes.NewReader(line))
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	fields, err := r.Read()
	if err != nil {
		return Sample{}, fmt.Errorf("parser: read csv: %w", err)
	}
	if len(fields) < minFields {
		return Sample{}, fmt.Errorf("parser: expected >=%d fields, got %d", minFields, len(fields))
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(fields[colValue]), 64)
	if err != nil {
		return Sample{}, fmt.Errorf("parser: parse value %q: %w", fields[colValue], err)
	}
	uuid := strings.TrimSpace(fields[colUUID])
	if uuid == "" {
		return Sample{}, fmt.Errorf("parser: empty uuid")
	}
	return Sample{
		Timestamp: p.Now(),
		Metric:    fields[colMetric],
		Value:     value,
		UUID:      uuid,
		GPUIndex:  fields[colGPUID],
		Device:    fields[colDevice],
		ModelName: fields[colModel],
		Hostname:  fields[colHostname],
	}, nil
}

