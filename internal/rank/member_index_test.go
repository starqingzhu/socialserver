package rankservice

import "testing"

func TestMemberIndexTrackAndLookup(t *testing.T) {
	idx := NewMemberIndex(nil, 0)
	entry := MemberEntry{BizType: "balloon", ActID: 1, GroupID: 1}
	idx.Track(1001, entry)

	entries := idx.Lookup(1001)
	if len(entries) != 1 || entries[0] != entry {
		t.Fatalf("expected one entry %+v, got %+v", entry, entries)
	}
	if idx.Lookup(9999) != nil {
		t.Fatalf("expected nil for unknown user")
	}
}

func TestMemberIndexTrackIdempotent(t *testing.T) {
	idx := NewMemberIndex(nil, 0)
	entry := MemberEntry{BizType: "balloon", ActID: 1, GroupID: 1}
	idx.Track(1001, entry)
	idx.Track(1001, entry)
	idx.Track(1001, entry)

	entries := idx.Lookup(1001)
	if len(entries) != 1 {
		t.Fatalf("expected idempotent track, got %d entries", len(entries))
	}
}

func TestMemberIndexMultiEntries(t *testing.T) {
	idx := NewMemberIndex(nil, 0)
	e1 := MemberEntry{BizType: "balloon", ActID: 1, GroupID: 1}
	e2 := MemberEntry{BizType: "balloon", ActID: 1, GroupID: 3}
	idx.Track(1001, e1)
	idx.Track(1001, e2)

	entries := idx.Lookup(1001)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestMemberIndexLookupByBizType(t *testing.T) {
	idx := NewMemberIndex(nil, 0)
	e1 := MemberEntry{BizType: "balloon", ActID: 1, GroupID: 1}
	e2 := MemberEntry{BizType: "charm", ActID: 2, GroupID: 1}
	idx.Track(1001, e1)
	idx.Track(1001, e2)

	balloonEntries := idx.LookupByBizType(1001, "balloon")
	if len(balloonEntries) != 1 || balloonEntries[0] != e1 {
		t.Fatalf("expected balloon entry, got %+v", balloonEntries)
	}
	charm := idx.LookupByBizType(1001, "charm")
	if len(charm) != 1 || charm[0] != e2 {
		t.Fatalf("expected charm entry, got %+v", charm)
	}
	unknown := idx.LookupByBizType(1001, "unknown")
	if len(unknown) != 0 {
		t.Fatalf("expected empty for unknown biz type, got %+v", unknown)
	}
}

func TestMemberIndexLookupReturnsCopy(t *testing.T) {
	idx := NewMemberIndex(nil, 0)
	entry := MemberEntry{BizType: "balloon", ActID: 1, GroupID: 1}
	idx.Track(1001, entry)

	entries := idx.Lookup(1001)
	entries[0].GroupID = 999

	original := idx.Lookup(1001)
	if original[0].GroupID != 1 {
		t.Fatalf("Lookup should return a copy, but original was mutated")
	}
}
