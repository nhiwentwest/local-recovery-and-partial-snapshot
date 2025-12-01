package opb

import (
	"encoding/json"
	"sort"
)

// VectorClock represents a causal vector clock.
// Keys are process/operator identifiers; values are monotonically increasing counters.
// Zero value is a nil map and behaves like an empty clock.
type VectorClock map[string]uint64

// NewVectorClock creates a new empty vector clock.
func NewVectorClock() VectorClock { return make(VectorClock) }

// Copy returns a deep copy of the vector clock.
func (vc VectorClock) Copy() VectorClock {
	if vc == nil {
		return make(VectorClock)
	}
	out := make(VectorClock, len(vc))
	for k, v := range vc {
		out[k] = v
	}
	return out
}

// Tick increments the counter for id by 1 and returns the clock for chaining.
func (vc VectorClock) Tick(id string) VectorClock {
	if vc == nil {
		vc = make(VectorClock)
	}
	vc[id] = vc[id] + 1
	return vc
}

// Merge updates this clock with the element-wise max of this and other and returns itself.
func (vc VectorClock) Merge(other VectorClock) VectorClock {
	if other == nil {
		return vc
	}
	if vc == nil {
		vc = make(VectorClock)
	}
	for k, ov := range other {
		if cv, ok := vc[k]; !ok || ov > cv {
			vc[k] = ov
		}
	}
	return vc
}

// LessEq returns true if vc <= other for all components (element-wise).
func (vc VectorClock) LessEq(other VectorClock) bool {
	for k, v := range vc {
		if ov := other[k]; v > ov {
			return false
		}
	}
	return true
}

// Equal returns true if both clocks are equal component-wise.
func (vc VectorClock) Equal(other VectorClock) bool {
	if len(vc) != len(other) {
		// quick fail; still need to compare as omitted zero entries are treated as 0
		// but in practice we require explicit equality on keys
	}
	// Compare both directions
	return vc.LessEq(other) && other.LessEq(vc)
}

// HappensBefore returns true if vc < other (vc <= other and vc != other).
func (vc VectorClock) HappensBefore(other VectorClock) bool {
	return vc.LessEq(other) && !vc.Equal(other)
}

// Dominates returns true if vc > other (vc >= other and vc != other).
func (vc VectorClock) Dominates(other VectorClock) bool {
	return other.LessEq(vc) && !vc.Equal(other)
}

// Concurrent returns true if neither vc <= other nor other <= vc.
func (vc VectorClock) Concurrent(other VectorClock) bool {
	return !(vc.LessEq(other) || other.LessEq(vc))
}

// MarshalJSON encodes the vector clock as compact JSON: {"id":counter, ...}.
func (vc VectorClock) MarshalJSON() ([]byte, error) {
	if vc == nil {
		return []byte("{}"), nil
	}
	// Use standard map encoding
	return json.Marshal(map[string]uint64(vc))
}

// UnmarshalJSON decodes from JSON representation produced by MarshalJSON.
func (vc *VectorClock) UnmarshalJSON(b []byte) error {
	var tmp map[string]uint64
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	m := make(VectorClock, len(tmp))
	for k, v := range tmp {
		m[k] = v
	}
	*vc = m
	return nil
}

// String returns a stable string representation useful for debugging, e.g. "a:1,b:3".
func (vc VectorClock) String() string {
	if len(vc) == 0 {
		return ""
	}
	keys := make([]string, 0, len(vc))
	for k := range vc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]byte, 0, len(keys)*6)
	for i, k := range keys {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, k...)
		out = append(out, ':')
		// encode uint64 decimal
		out = append(out, itoa10(vc[k])...)
	}
	return string(out)
}

// itoa10 converts a uint64 to decimal string without allocations beyond the buffer.
func itoa10(u uint64) string {
	if u == 0 {
		return "0"
	}
	var buf [20]byte // uint64 max 20 digits
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + (u % 10))
		u /= 10
	}
	return string(buf[i:])
}
