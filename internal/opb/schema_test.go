package opb

import "testing"

func TestWindowStart(t *testing.T) {
	norm := int64(1694500010) // falls into window starting at 1694499900 when window=300s
	got := WindowStart(norm, 300)
	want := int64(1694499900)
	if got != want {
		t.Fatalf("WindowStart: got=%d want=%d", got, want)
	}
}

func TestWindowStart_DefaultsTo300(t *testing.T) {
	norm := int64(1005)
	if got, want := WindowStart(norm, 0), int64(900); got != want {
		t.Fatalf("WindowStart default 300: got=%d want=%d", got, want)
	}
	if got, want := WindowStart(norm, -1), int64(900); got != want {
		t.Fatalf("WindowStart negative -> default 300: got=%d want=%d", got, want)
	}
}

func TestOutputKey(t *testing.T) {
	got := OutputKey("A", "p1", 1694499900)
	want := "A#p1#1694499900"
	if got != want {
		t.Fatalf("OutputKey: got=%s want=%s", got, want)
	}
}
