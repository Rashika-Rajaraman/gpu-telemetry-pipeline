// Package group implements consumer-group coordination: membership, partition
// assignment (recomputed on every join/leave so load rebalances as consumers scale
// up/down), and committed-offset tracking per partition.
//
// Delivery position is intentionally NOT stored here. The broker delivers from a
// partition's committed offset and only advances it on ack; if a consumer dies
// before acking, the committed offset has not moved, so the partition's new owner
// redelivers from there. That yields at-least-once delivery with no extra state.
package group

import (
	"sort"
	"sync"
)

// Group tracks membership, assignment, and committed offsets for one consumer
// group over a topic with a fixed number of partitions. Safe for concurrent use.
type Group struct {
	name          string
	numPartitions int

	mu         sync.Mutex
	members    map[string]bool
	assignment map[int]string // partition -> consumerID ("" if unassigned)
	committed  map[int]int64  // partition -> next offset to deliver (acked watermark)
	generation int            // bumped on every rebalance
}

// New returns an empty group coordinating numPartitions partitions.
func New(name string, numPartitions int) *Group {
	g := &Group{
		name:          name,
		numPartitions: numPartitions,
		members:       make(map[string]bool),
		assignment:    make(map[int]string, numPartitions),
		committed:     make(map[int]int64, numPartitions),
	}
	for p := 0; p < numPartitions; p++ {
		g.assignment[p] = ""
	}
	return g
}

// Name returns the group name.
func (g *Group) Name() string { return g.name }

// Join adds a consumer (idempotent) and rebalances. It returns the new generation.
func (g *Group) Join(consumerID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.members[consumerID] {
		g.members[consumerID] = true
		g.rebalanceLocked()
	}
	return g.generation
}

// Leave removes a consumer (idempotent) and rebalances its partitions onto the
// remaining members. It returns the new generation.
func (g *Group) Leave(consumerID string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.members[consumerID] {
		delete(g.members, consumerID)
		g.rebalanceLocked()
	}
	return g.generation
}

// rebalanceLocked distributes partitions round-robin across the sorted member set,
// giving each member either floor(N/M) or ceil(N/M) partitions. Deterministic so
// all parties compute the same assignment.
func (g *Group) rebalanceLocked() {
	g.generation++
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for p := 0; p < g.numPartitions; p++ {
		if len(ids) == 0 {
			g.assignment[p] = ""
			continue
		}
		g.assignment[p] = ids[p%len(ids)]
	}
}

// Assignment returns the sorted partitions currently owned by consumerID.
func (g *Group) Assignment(consumerID string) []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	var parts []int
	for p := 0; p < g.numPartitions; p++ {
		if g.assignment[p] == consumerID {
			parts = append(parts, p)
		}
	}
	return parts
}

// Owner returns the consumerID that currently owns the partition ("" if none).
func (g *Group) Owner(partition int) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.assignment[partition]
}

// Commit advances the committed offset for a partition. Lower or equal offsets are
// ignored so out-of-order commits never move the watermark backward.
func (g *Group) Commit(partition int, nextOffset int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if nextOffset > g.committed[partition] {
		g.committed[partition] = nextOffset
	}
}

// Committed returns the committed (next-to-deliver) offset for a partition.
func (g *Group) Committed(partition int) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.committed[partition]
}

// Members returns the sorted list of current members.
func (g *Group) Members() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	ids := make([]string, 0, len(g.members))
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Generation returns the current rebalance generation.
func (g *Group) Generation() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation
}
