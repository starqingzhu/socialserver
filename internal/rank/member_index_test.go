package rankservice

import "testing"

func TestMemberIndexTrackAndLookup(t *testing.T) {
	idx := NewMemberIndex(nil)
	entry := MemberEntry{BizType: BizTypeBalloon, ActID: 1, GroupID: 1}
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
	idx := NewMemberIndex(nil)
	entry := MemberEntry{BizType: BizTypeBalloon, ActID: 1, GroupID: 1}
	idx.Track(1001, entry)
	idx.Track(1001, entry)
	idx.Track(1001, entry)

	entries := idx.Lookup(1001)
	if len(entries) != 1 {
		t.Fatalf("expected idempotent track, got %d entries", len(entries))
	}
}

func TestMemberIndexMultiEntries(t *testing.T) {
	idx := NewMemberIndex(nil)
	e1 := MemberEntry{BizType: BizTypeBalloon, ActID: 1, GroupID: 1}
	e2 := MemberEntry{BizType: BizTypeBalloon, ActID: 1, GroupID: 3}
	idx.Track(1001, e1)
	idx.Track(1001, e2)

	entries := idx.Lookup(1001)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}

func TestMemberIndexLookupByBizType(t *testing.T) {
	idx := NewMemberIndex(nil)
	e1 := MemberEntry{BizType: BizTypeBalloon, ActID: 1, GroupID: 1}
	e2 := MemberEntry{BizType: "charm", ActID: 2, GroupID: 1}
	idx.Track(1001, e1)
	idx.Track(1001, e2)

	balloonEntries := idx.LookupByBizType(1001, BizTypeBalloon)
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

func TestMemberIndexRemoveByKey(t *testing.T) {
	idx := NewMemberIndex(nil)
	e1 := MemberEntry{BizType: BizTypeBalloon, ActID: 1, GroupID: 1}
	e2 := MemberEntry{BizType: "charm", ActID: 2, GroupID: 2}
	idx.Track(1001, e1)
	idx.Track(1001, e2)
	idx.Track(2001, e1)

	idx.RemoveByKey(NewBizKey(BizTypeBalloon, 1).String())

	entries1001 := idx.Lookup(1001)
	if len(entries1001) != 1 || entries1001[0].BizType != "charm" {
		t.Fatalf("expected only charm entry for 1001, got %+v", entries1001)
	}
	entries2001 := idx.Lookup(2001)
	if len(entries2001) != 0 {
		t.Fatalf("expected 0 entries for 2001 after remove, got %d", len(entries2001))
	}
}

func TestMemberIndexLookupReturnsCopy(t *testing.T) {
	idx := NewMemberIndex(nil)
	entry := MemberEntry{BizType: BizTypeBalloon, ActID: 1, GroupID: 1}
	idx.Track(1001, entry)

	entries := idx.Lookup(1001)
	entries[0].GroupID = 999

	original := idx.Lookup(1001)
	if original[0].GroupID != 1 {
		t.Fatalf("Lookup should return a copy, but original was mutated")
	}
}
