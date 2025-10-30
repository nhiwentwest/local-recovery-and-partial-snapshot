package main

import "testing"

func TestMurmur2PartitionDeterministicAndInRange(t *testing.T) {
	keys := [][]byte{
		[]byte("A#p1#1694499900"),
		[]byte("A#p2#1694499900"),
		[]byte("B#p1#1694499900"),
		[]byte("store-xyz#prod-123#1000"),
		[]byte("store-xyz#prod-123#1005"),
	}
	partitions := 6
	for _, k := range keys {
		p1 := partitionForKey(k, partitions)
		if p1 < 0 || int(p1) >= partitions {
			t.Fatalf("partition out of range: got=%d partitions=%d key=%q", p1, partitions, string(k))
		}
		// Determinism: same key yields same partition
		p2 := partitionForKey(k, partitions)
		if p1 != p2 {
			t.Fatalf("partition not deterministic: %d vs %d for key=%q", p1, p2, string(k))
		}
	}
}

func TestMurmur2HashBasicSanity(t *testing.T) {
	// Different keys should not all collide; this is a weak sanity check.
	k1 := []byte("A#p1#1694499900")
	k2 := []byte("A#p1#1694500200")
	if murmur2(k1) == murmur2(k2) {
		t.Fatalf("unexpected identical murmur2 hash for different keys")
	}
}
