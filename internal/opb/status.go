package opb

import (
	"sync/atomic"
	"time"
)

// AppStatus holds the application's lifecycle state for observability.
type AppStatus struct {
	Status              string `json:"status"`
	Instance            string `json:"instance"`
	GroupID             string `json:"groupId"`
	WindowSizeSec       int    `json:"windowSizeSec"`
	TTRMs               int64  `json:"ttrMs"`
	RestoringSnapshotID string `json:"restoringSnapshotId"`
	LastChangelogOffset int64  `json:"lastChangelogOffset"`
	LastRestoreApplied  int64  `json:"lastRestoreApplied"`
	LastRestoreSkipped  int64  `json:"lastRestoreSkipped"`
	// Fast-path EOS counters snapshot for REST
	EventsApplied      int64 `json:"eventsApplied"`
	EventsSkippedDedup int64 `json:"eventsSkippedDedup"`
	EventsSkippedSeq   int64 `json:"eventsSkippedSeq"`
	// Cluster assignment & lag snapshot
	Topic           string  `json:"topic"`
	Partitions      []int   `json:"partitions"`
	LagTotal        float64 `json:"lagTotal"`
	RebalanceStatus string  `json:"rebalanceStatus,omitempty"`
	CausalInflight  int     `json:"causalInflight,omitempty"`
	CausalReplay    int64   `json:"causalReplayTotal,omitempty"`
	CausalCutID     string  `json:"causalCutId,omitempty"`
	CausalPhase     string  `json:"causalPhase,omitempty"`
	CausalMarkers   int     `json:"causalMarkersSeen,omitempty"`
	CausalMarkersOf int     `json:"causalMarkersTotal,omitempty"`
}

// StatusManager manages the application status atomically.
type StatusManager struct {
	val atomic.Value
}

func (s *StatusManager) with(fn func(AppStatus) AppStatus) {
	cur := s.val.Load().(AppStatus)
	s.val.Store(fn(cur))
}

func (s *StatusManager) IncEventsApplied(n int64) {
	s.with(func(a AppStatus) AppStatus { a.EventsApplied += n; return a })
}
func (s *StatusManager) IncEventsSkippedDedup(n int64) {
	s.with(func(a AppStatus) AppStatus { a.EventsSkippedDedup += n; return a })
}
func (s *StatusManager) IncEventsSkippedSeq(n int64) {
	s.with(func(a AppStatus) AppStatus { a.EventsSkippedSeq += n; return a })
}

// NewStatusManager creates a new status manager.
func NewStatusManager(instanceID, groupID string, windowSizeSec int) *StatusManager {
	s := &StatusManager{}
	s.val.Store(AppStatus{
		Status:        "starting",
		Instance:      instanceID,
		GroupID:       groupID,
		WindowSizeSec: windowSizeSec,
	})
	return s
}

// SetHealthy sets the status to healthy.
func (s *StatusManager) SetHealthy() {
	cur := s.val.Load().(AppStatus)
	cur.Status = "healthy"
	s.val.Store(cur)
}

// SetRecovering sets the status to recovering and records restore details.
func (s *StatusManager) SetRecovering(snapshotID string, offset int64) {
	cur := s.val.Load().(AppStatus)
	cur.Status = "recovering"
	cur.RestoringSnapshotID = snapshotID
	cur.LastChangelogOffset = offset
	s.val.Store(cur)
}

// SetRecovered sets the status to healthy after recovery and records TTR.
func (s *StatusManager) SetRecovered(ttr time.Duration, applied, skipped int64) {
	cur := s.val.Load().(AppStatus)
	cur.Status = "healthy"
	cur.TTRMs = ttr.Milliseconds()
	cur.LastRestoreApplied = applied
	cur.LastRestoreSkipped = skipped
	s.val.Store(cur)
}

// SetAssignment updates topic and partitions assigned to this instance.
func (s *StatusManager) SetAssignment(topic string, parts []int) {
	s.with(func(a AppStatus) AppStatus {
		a.Topic = topic
		// make a copy to avoid races if caller reuses slice
		cp := make([]int, len(parts))
		copy(cp, parts)
		a.Partitions = cp
		return a
	})
}

// SetLagTotal updates the total lag snapshot.
func (s *StatusManager) SetLagTotal(v float64) {
	s.with(func(a AppStatus) AppStatus { a.LagTotal = v; return a })
}

// SetRebalanceStatus updates the consumer group rebalance status.
func (s *StatusManager) SetRebalanceStatus(st string) {
	s.with(func(a AppStatus) AppStatus { a.RebalanceStatus = st; return a })
}

func (s *StatusManager) SetCausalInflight(v int) {
	s.with(func(a AppStatus) AppStatus { a.CausalInflight = v; return a })
}

func (s *StatusManager) AddCausalReplay(n int64) {
	s.with(func(a AppStatus) AppStatus { a.CausalReplay += n; return a })
}

func (s *StatusManager) SetCausalReplay(n int64) {
	s.with(func(a AppStatus) AppStatus { a.CausalReplay = n; return a })
}

func (s *StatusManager) BeginCausalCut(id string, total int) {
	s.with(func(a AppStatus) AppStatus {
		a.CausalCutID = id
		a.CausalPhase = "tracking"
		a.CausalMarkersOf = total
		a.CausalMarkers = 0
		return a
	})
}

func (s *StatusManager) SetCausalPhase(phase string) {
	s.with(func(a AppStatus) AppStatus { a.CausalPhase = phase; return a })
}

func (s *StatusManager) SetCausalMarkers(seen int) {
	s.with(func(a AppStatus) AppStatus {
		a.CausalMarkers = seen
		if seen >= a.CausalMarkersOf && a.CausalMarkersOf > 0 {
			a.CausalMarkers = a.CausalMarkersOf
		}
		return a
	})
}

func (s *StatusManager) ClearCausalCut() {
	s.with(func(a AppStatus) AppStatus {
		a.CausalCutID = ""
		a.CausalPhase = ""
		a.CausalMarkers = 0
		a.CausalMarkersOf = 0
		a.CausalInflight = 0
		return a
	})
}

func (s *StatusManager) ApplyRestoreHistory(ttrMs int64, snapshotID string, offset, applied, skipped int64) {
	s.with(func(a AppStatus) AppStatus {
		a.TTRMs = ttrMs
		a.RestoringSnapshotID = snapshotID
		a.LastChangelogOffset = offset
		a.LastRestoreApplied = applied
		a.LastRestoreSkipped = skipped
		return a
	})
}

// Load returns the current status.
func (s *StatusManager) Load() AppStatus {
	return s.val.Load().(AppStatus)
}
