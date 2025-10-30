package restore

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"hpb/internal/manifest"
	"hpb/internal/state"
)

func TestParseAndApplyLine_Basics(t *testing.T) {
	st := state.NewInMemoryStore()
	r := NewRestorer(st, nil, manifest.NewFilesystemManifest(t.TempDir()), "")
	// apply seq=1
	line := []byte(`{"key":"K#1","seq":1,"delta":5}`)
	a, s, err := r.parseAndApplyLine(line)
	if err != nil || !a || s {
		t.Fatalf("apply seq1 failed: a=%v s=%v err=%v", a, s, err)
	}
	// duplicate seq=1 -> skip
	line = []byte(`{"key":"K#1","seq":1,"delta":9}`)
	a, s, err = r.parseAndApplyLine(line)
	if err != nil || a || !s {
		t.Fatalf("dup seq1 should skip: a=%v s=%v err=%v", a, s, err)
	}
	// seq gap allowed -> apply
	line = []byte(`{"key":"K#1","seq":3,"delta":1}`)
	a, s, err = r.parseAndApplyLine(line)
	if err != nil || !a || s {
		t.Fatalf("seq gap should apply: a=%v s=%v err=%v", a, s, err)
	}
}

func TestReplayLines_TableDriven(t *testing.T) {
	cases := []struct {
		name    string
		content string
		from    int64
		wantA   int
		wantS   int
		wantErr bool
	}{
		{"offset0_mixed", strings.Join([]string{
			`{"key":"K#1","seq":1,"delta":1}`,
			`{"key":"K#1","seq":1,"delta":9}`,
			`{"key":"K#2","seq":1,"delta":1}`,
			`{"key":"K#1","seq":2,"delta":1}`,
		}, "\n") + "\n", 0, 3, 1, false},
		{"offset2_on5", strings.Join([]string{
			`{"key":"K#1","seq":1,"delta":1}`,
			`{"key":"K#1","seq":2,"delta":1}`,
			`{"key":"K#1","seq":3,"delta":1}`,
			`{"key":"K#1","seq":4,"delta":1}`,
			`{"key":"K#1","seq":5,"delta":1}`,
		}, "\n") + "\n", 2, 3, 0, false},
		{"malformed_first", "{bad}\n", 0, 0, 0, true},
		{"malformed_last", strings.Join([]string{
			`{"key":"K#1","seq":1,"delta":1}`,
			`{bad}`,
		}, "\n") + "\n", 0, 0, 0, true},
		{"empty_line_mid", strings.Join([]string{
			`{"key":"K#1","seq":1,"delta":1}`,
			"",
			`{"key":"K#1","seq":2,"delta":1}`,
		}, "\n") + "\n", 0, 0, 0, true},
		{"near_limit_ok", func() string {
			// a line well under scanner limit to avoid env variance
			long := strings.Repeat("A", 1000)
			return fmt.Sprintf("{\"key\":\"K#1\",\"seq\":1,\"delta\":1,\"pad\":\"%s\"}", long) + "\n"
		}(), 0, 1, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := state.NewInMemoryStore()
			r := NewRestorer(st, nil, manifest.NewFilesystemManifest(t.TempDir()), "")
			res := r.replayLines(bytes.NewBufferString(tc.content), tc.from)
			if (res.Error != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", res.Error, tc.wantErr)
			}
			if res.Applied != tc.wantA || res.Skipped != tc.wantS {
				t.Fatalf("applied/skipped got %d/%d want %d/%d", res.Applied, res.Skipped, tc.wantA, tc.wantS)
			}
		})
	}
}
