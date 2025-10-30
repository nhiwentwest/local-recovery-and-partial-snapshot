package opb

import (
	"errors"

	"hpb/internal/state"
)

// Entry represents a key-state pair for iteration output.
type Entry struct {
	Key   string
	State state.RecordState
}

var errStop = errors.New("stop iteration")

// Iterate returns up to limit entries satisfying filter (if non-nil).
// If limit <= 0, it returns all matching entries.
func Iterate(st state.Store, limit int, filter func(string, state.RecordState) bool) ([]Entry, error) {
	remaining := limit
	out := make([]Entry, 0)
	stop := false
	err := st.Range(func(k string, rs state.RecordState) error {
		if stop {
			return errStop
		}
		if filter != nil && !filter(k, rs) {
			return nil
		}
		out = append(out, Entry{Key: k, State: rs})
		if limit > 0 {
			remaining--
			if remaining == 0 {
				stop = true
				return errStop
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStop) {
		return nil, err
	}
	return out, nil
}
