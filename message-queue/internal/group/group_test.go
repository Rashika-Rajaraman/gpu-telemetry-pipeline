package group

import (
	"reflect"
	"testing"
)

func TestSingleMemberOwnsAllPartitions(t *testing.T) {
	g := New("collectors", 8)
	g.Join("c1")
	got := g.Assignment("c1")
	want := []int{0, 1, 2, 3, 4, 5, 6, 7}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assignment = %v, want %v", got, want)
	}
}

func TestRebalanceEvenAndDisjoint(t *testing.T) {
	g := New("collectors", 16)
	for _, id := range []string{"c1", "c2", "c3"} {
		g.Join(id)
	}
	seen := map[int]string{}
	for _, id := range []string{"c1", "c2", "c3"} {
		parts := g.Assignment(id)
		// Each member should own 5 or 6 of 16 partitions.
		if len(parts) < 5 || len(parts) > 6 {
			t.Fatalf("%s owns %d partitions, want 5-6", id, len(parts))
		}
		for _, p := range parts {
			if other, dup := seen[p]; dup {
				t.Fatalf("partition %d assigned to both %s and %s", p, other, id)
			}
			seen[p] = id
		}
	}
	if len(seen) != 16 {
		t.Fatalf("covered %d partitions, want 16", len(seen))
	}
}

func TestLeaveReassignsOrphanedPartitions(t *testing.T) {
	g := New("collectors", 8)
	g.Join("c1")
	g.Join("c2")
	gen := g.Generation()

	c1Before := g.Assignment("c1")
	g.Leave("c2")

	if g.Generation() <= gen {
		t.Fatalf("generation did not advance on leave (%d)", g.Generation())
	}
	// c1 now owns everything.
	if got := g.Assignment("c1"); len(got) != 8 {
		t.Fatalf("after leave c1 owns %d, want 8", len(got))
	}
	// Sanity: c1 kept at least its prior partitions.
	for _, p := range c1Before {
		if g.Owner(p) != "c1" {
			t.Fatalf("partition %d no longer owned by c1", p)
		}
	}
}

func TestJoinIsIdempotent(t *testing.T) {
	g := New("g", 4)
	gen1 := g.Join("c1")
	gen2 := g.Join("c1") // duplicate join
	if gen1 != gen2 {
		t.Fatalf("duplicate join changed generation %d -> %d", gen1, gen2)
	}
}

func TestCommitMonotonic(t *testing.T) {
	g := New("g", 4)
	g.Commit(2, 10)
	g.Commit(2, 5) // stale, ignored
	if got := g.Committed(2); got != 10 {
		t.Fatalf("committed = %d, want 10", got)
	}
	g.Commit(2, 20)
	if got := g.Committed(2); got != 20 {
		t.Fatalf("committed = %d, want 20", got)
	}
}

func TestDeterministicAssignment(t *testing.T) {
	// Two independently built groups with the same members must agree.
	a := New("g", 10)
	b := New("g", 10)
	for _, id := range []string{"z", "a", "m"} {
		a.Join(id)
	}
	for _, id := range []string{"m", "z", "a"} { // different join order
		b.Join(id)
	}
	for p := 0; p < 10; p++ {
		if a.Owner(p) != b.Owner(p) {
			t.Fatalf("partition %d: a=%s b=%s", p, a.Owner(p), b.Owner(p))
		}
	}
}

func TestGroupNameAndMembers(t *testing.T) {
	g := New("collectors", 4)
	if g.Name() != "collectors" {
		t.Errorf("Name = %q", g.Name())
	}
	g.Join("c2")
	g.Join("c1")
	got := g.Members()
	if len(got) != 2 || got[0] != "c1" || got[1] != "c2" {
		t.Fatalf("Members = %v, want sorted [c1 c2]", got)
	}
}

func TestGroupEmptyAfterAllLeave(t *testing.T) {
	g := New("g", 4)
	g.Join("c1")
	g.Leave("c1")
	for p := 0; p < 4; p++ {
		if o := g.Owner(p); o != "" {
			t.Fatalf("partition %d owner = %q, want empty", p, o)
		}
	}
	if len(g.Members()) != 0 {
		t.Fatalf("members = %v, want empty", g.Members())
	}
}
