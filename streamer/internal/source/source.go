// Package source reads DCGM telemetry rows from a CSV file and loops over them to
// simulate a continuous telemetry stream. It also extracts the GPU uuid from each
// row, which the publisher uses as the message partition key so that all telemetry
// for a given GPU is ordered on a single partition.
package source

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

// canonicalUUIDColumn is the uuid column index in the standard DCGM export, used
// as a fallback when the header does not name a "uuid" column.
const canonicalUUIDColumn = 4

// maxLineBytes bounds a single CSV line to guard against pathological input.
const maxLineBytes = 4 * 1024 * 1024

// Source streams data rows from a DCGM CSV file. When loop is set, reaching EOF
// reopens the file (skipping the header) so the stream never ends.
type Source struct {
	path    string
	loop    bool
	f       *os.File
	sc      *bufio.Scanner
	uuidIdx int
	log     *logrus.Entry
}

// Open opens the CSV at path, reads its header, and locates the uuid column. If
// loop is true the source restarts from the top at EOF. A nil logger falls back to
// the logrus standard logger.
func Open(path string, loop bool, logger *logrus.Logger) (*Source, error) {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	s := &Source{
		path: path,
		loop: loop,
		log:  logger.WithField("component", "source"),
	}
	if err := s.openAndReadHeader(); err != nil {
		return nil, err
	}
	s.log.WithFields(logrus.Fields{
		"path":     path,
		"loop":     loop,
		"uuid_col": s.uuidIdx,
	}).Info("opened telemetry source")
	return s, nil
}

func (s *Source) openAndReadHeader() error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("source: open %s: %w", s.path, err)
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	if !sc.Scan() {
		f.Close()
		if err := sc.Err(); err != nil {
			return fmt.Errorf("source: read header: %w", err)
		}
		return fmt.Errorf("source: empty file %s", s.path)
	}
	header, err := parseCSVLine(sc.Text())
	if err != nil {
		f.Close()
		return fmt.Errorf("source: parse header: %w", err)
	}
	s.f = f
	s.sc = sc
	s.uuidIdx = indexOf(header, "uuid")
	if s.uuidIdx < 0 {
		s.uuidIdx = canonicalUUIDColumn
	}
	return nil
}

// Next returns the next data row as the raw CSV line plus the GPU uuid key.
// Malformed or blank rows are skipped. When loop is false it returns io.EOF at the
// end of the file.
func (s *Source) Next() (line, key []byte, err error) {
	for {
		if s.sc.Scan() {
			line, key, ok := s.parseRow(s.sc.Text())
			if !ok {
				continue // skip blank or malformed rows
			}
			return line, key, nil
		}
		if scanErr := s.sc.Err(); scanErr != nil {
			return nil, nil, fmt.Errorf("source: scan: %w", scanErr)
		}
		s.f.Close()
		if !s.loop {
			return nil, nil, io.EOF
		}
		s.log.Debug("reached end of file, restarting stream")
		if err := s.openAndReadHeader(); err != nil {
			return nil, nil, err
		}
	}
}

// parseRow validates a single line and extracts its uuid key. It returns ok=false
// for blank or malformed rows so the caller can skip them.
func (s *Source) parseRow(text string) (line, key []byte, ok bool) {
	if strings.TrimSpace(text) == "" {
		return nil, nil, false
	}
	fields, err := parseCSVLine(text)
	if err != nil {
		s.log.WithError(err).Debug("skipping malformed row")
		return nil, nil, false
	}
	if s.uuidIdx < len(fields) {
		key = []byte(fields[s.uuidIdx])
	}
	return []byte(text), key, true
}

// Close releases the underlying file.
func (s *Source) Close() error {
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

// parseCSVLine splits one CSV line, tolerating quoted fields and variable counts.
func parseCSVLine(line string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(line))
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	return r.Read()
}

// indexOf returns the index of the named column (case-insensitive) or -1.
func indexOf(header []string, name string) int {
	for i, h := range header {
		if strings.EqualFold(strings.TrimSpace(h), name) {
			return i
		}
	}
	return -1
}

