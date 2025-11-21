package state

import (
	"strconv"
	"testing"
)

func BenchmarkInMemoryApply(b *testing.B) {
	s := NewInMemoryStore()
	s.SetInstanceID("B1")
	// Pre-create keys to avoid map growth during bench
	for i := 0; i < 10000; i++ {
		k := "S#p1#" + strconv.Itoa(100+i%10)
		_, _, _ = s.Apply(k, 0, 0, 0)
	}
	b.ResetTimer()
	var seq int64
	for i := 0; i < b.N; i++ {
		k := "S#p1#" + strconv.Itoa(100+i%10)
		seq++
		_, _, _ = s.Apply(k, 10, 1, seq)
	}
}

