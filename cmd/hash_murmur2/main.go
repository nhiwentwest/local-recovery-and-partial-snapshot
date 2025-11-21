package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Kafka's default partitioner uses Murmur2 with unsigned 32-bit output, seed 0x9747b28c
// Implementation adapted to match Kafka's Java murmur2 implementation semantics
func murmur2(data []byte) uint32 {
	const seed uint32 = 0x9747b28c
	const m uint32 = 0x5bd1e995
	const r uint32 = 24

	length := uint32(len(data))
	h := seed ^ length
	offset := 0
	for length >= 4 {
		k := binary.LittleEndian.Uint32(data[offset : offset+4])
		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k

		offset += 4
		length -= 4
	}

	switch length {
	case 3:
		h ^= uint32(data[offset+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[offset+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[offset])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: hash_murmur2 <key> <partitions>")
		os.Exit(2)
	}
	key := []byte(os.Args[1])
	var parts uint32
	// parse partitions
	var p64 uint64
	for i := 0; i < len(os.Args[2]); i++ {
		c := os.Args[2][i]
		if c < '0' || c > '9' {
			fmt.Fprintln(os.Stderr, "invalid partitions")
			os.Exit(2)
		}
		p64 = p64*10 + uint64(c-'0')
	}
	if p64 == 0 {
		fmt.Fprintln(os.Stderr, "partitions must be > 0")
		os.Exit(2)
	}
	parts = uint32(p64)

	h := murmur2(key)
	// Kafka uses positive (h & 0x7fffffff)
	pos := (h & 0x7fffffff) % parts
	fmt.Println(pos)
}
