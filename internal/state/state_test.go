package state

import "testing"

func TestApply_SeqRules(t *testing.T) {
	s := NewInMemoryStore()

	applied, st, err := s.Apply("k", 10, 1, 1)
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
	applied, st, err = s.Apply("k", 20, 2, 1)
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
	applied, st, err = s.Apply("k", 30, 3, 3)
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
