package state

import (
	"testing"
	"time"
)

func TestApply_SeqRules(t *testing.T) {
	s := NewInMemoryStore()

	applied, st, err := s.Apply("k", 10, 1, 1, SourceUnspecified)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatalf("first apply should apply")
	}
	if st.LastSeq != 1 || st.SumAmount != 10 || st.SumQty != 1 {
		t.Fatalf("unexpected state after first apply: %+v", st)
	}

	// Lower or equal seq should not apply (idempotency)
	applied, st, err = s.Apply("k", 20, 2, 1, SourceUnspecified)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied {
		t.Fatalf("apply with same seq should not apply")
	}
	if st.LastSeq != 1 || st.SumAmount != 10 || st.SumQty != 1 {
		t.Fatalf("state should be unchanged: %+v", st)
	}

	// Gap allowed in Phase 1
	applied, st, err = s.Apply("k", 30, 3, 3, SourceUnspecified)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !applied {
		t.Fatalf("gap apply should apply in phase 1")
	}
	if st.LastSeq != 3 || st.SumAmount != 40 || st.SumQty != 4 {
		t.Fatalf("unexpected state after gap: %+v", st)
	}
}

func TestInMemoryStore_Range_LockShortAndReadOnly(t *testing.T) {
	s := NewInMemoryStore()
	s.Apply("a1", 10, 1, 1, SourceUnspecified)
	s.Apply("b2", 20, 2, 2, SourceUnspecified)
	s.Apply("c3", 30, 3, 3, SourceUnspecified)

	// WARNING: Không bao giờ được gọi Apply từ trong callback Range!
	// Nếu gọi sẽ deadlock do RWMutex (Lock + RLock không upgrade được).

	// Thay vì mutate, kiểm tra Range với Apply trên new key chạy đồng thời không block nhau quá 100ms (lock đủ thoáng).
	done := make(chan struct{})
	start := time.Now()
	go func() {
		t0 := time.Now()
		s.Apply("d4", 40, 4, 4, SourceUnspecified)
		took := time.Since(t0)
		if took > 100*time.Millisecond {
			t.Errorf("Apply waited lock too long: %v", took)
		}
		done <- struct{}{}
	}()
	s.Range(func(key string, rs RecordState) error { time.Sleep(10 * time.Millisecond); return nil })
	<-done
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("Range+Apply concurrent took too long: %v", time.Since(start))
	}
}
