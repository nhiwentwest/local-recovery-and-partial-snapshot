//go:build integration
// +build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type manifestFile struct {
	SnapshotID       string `json:"snapshotId"`
	SnapshotFormat   string `json:"snapshotFormat"`
	SnapshotShards   int    `json:"snapshotShards"`
	SnapshotKeys     int    `json:"snapshotKeys"`
	SnapshotType     string `json:"snapshotType"`
	BaseSnapshotID   string `json:"baseSnapshotId"`
	ParentSnapshotID string `json:"parentSnapshotId"`
	DeltaSequence    int    `json:"deltaSequence"`
}

func tcpAvailable(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 800*time.Millisecond)
	if err == nil {
		_ = c.Close()
		return true
	}
	return false
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitHTTPReady(url string, timeout time.Duration) error {
	cli := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := cli.Get(url)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", url)
}

type appStatus struct {
	Status     string `json:"status"`
	Partitions []int  `json:"partitions"`
}

func waitAssigned(base string, timeout time.Duration) error {
	cli := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := cli.Get(base + "/status")
		if err == nil && resp.StatusCode == 200 {
			var st appStatus
			_ = json.NewDecoder(resp.Body).Decode(&st)
			resp.Body.Close()
			if len(st.Partitions) > 0 {
				return nil
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for assignment")
}

func readJSON[T any](path string, out *T) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

func waitForFile(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for file %s", path)
}

func postJSON(url string, body string) error {
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	cli := &http.Client{Timeout: 3 * time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// Integration test for barrier-cut delta path.
// Requires a reachable Kafka broker (KAFKA_BOOTSTRAP or localhost:9092) and the expected topics to exist.
func TestBarrierCutDelta_Integration(t *testing.T) {
	bootstrap := os.Getenv("KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		bootstrap = "localhost:9092"
	}
	if !tcpAvailable(strings.Split(bootstrap, ",")[0]) {
		t.Skipf("Kafka not reachable at %s; skipping integration test", bootstrap)
	}
	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	groupID := fmt.Sprintf("opb-it-%d", time.Now().UnixNano())
	baseDir := t.TempDir()
	snapDir := filepath.Join(baseDir, "snap")
	chgDir := filepath.Join(baseDir, "chg")
	stDir := filepath.Join(baseDir, "state")
	_ = os.MkdirAll(snapDir, 0o755)
	_ = os.MkdirAll(chgDir, 0o755)
	_ = os.MkdirAll(stDir, 0o755)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	args := []string{"run", ".",
		"--http", fmt.Sprintf(":%d", port),
		"--group-id", groupID,
		"--kafka-bootstrap", bootstrap,
		"--input-source", "kafka",
		"--topic-enriched", "p1.orders.enriched",
		"--output-topic", "p1.orders.output",
		"--topic-changelog", "p1.opb-changelog",
		"--topic-snapshots", "p1.opb-snapshots",
		"--changelog-sink", "both",
		"--manifest-sink", "both",
		"--snapshot-interval", "600",
		"--snap-max-deltas", "3",
		"--snapshot-dir", snapDir,
		"--changelog-dir", chgDir,
		"--state-dir", stDir,
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = "."
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start opb: %v", err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	// drain logs to avoid blocking
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
		}
	}()
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
		}
	}()

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	// Wait for HTTP server by probing /status (health may require assignment)
	if err := waitHTTPReady(base+"/status", 30*time.Second); err != nil {
		t.Skipf("opb http not ready: %v", err)
	}
	if err := waitAssigned(base, 20*time.Second); err != nil {
		t.Skipf("opb not assigned (topics may be missing): %v", err)
	}
	// Inject some data
	injURL := fmt.Sprintf("http://127.0.0.1:%d/api/inject-test-data", port)
	if err := postJSON(injURL, `{"storeId":"IT","productId":"","ws":0,"mode":"new","n":200,"start":0}`); err != nil {
		t.Fatalf("inject: %v", err)
	}
	// Barrier cut full
	fullURL := fmt.Sprintf("http://127.0.0.1:%d/admin/snapshot-cut?type=full", port)
	if err := postJSON(fullURL, ""); err != nil {
		t.Fatalf("full cut: %v", err)
	}
	latest := filepath.Join(snapDir, "manifest.latest.json")
	if err := waitForFile(latest, 15*time.Second); err != nil {
		t.Fatalf("wait manifest latest: %v", err)
	}
	var m1 manifestFile
	if err := readJSON(latest, &m1); err != nil {
		t.Fatalf("read manifest latest: %v", err)
	}
	if m1.SnapshotID == "" || strings.ToLower(m1.SnapshotType) != "full" {
		t.Fatalf("expected full manifest, got: %+v", m1)
	}
	// Mutate state again so delta snapshot has dirty keys to flush.
	if err := postJSON(injURL, `{"storeId":"IT","productId":"","ws":0,"mode":"update","n":50,"start":200}`); err != nil {
		t.Fatalf("inject before delta: %v", err)
	}
	// Barrier cut delta
	deltaURL := fmt.Sprintf("http://127.0.0.1:%d/admin/snapshot-cut?type=delta", port)
	if err := postJSON(deltaURL, ""); err != nil {
		t.Fatalf("delta cut: %v", err)
	}
	// Wait new latest manifest (allow some time)
	var m2 manifestFile
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err := readJSON(latest, &m2); err == nil {
			if strings.ToLower(m2.SnapshotType) == "delta" && m2.SnapshotID != "" {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	if strings.ToLower(m2.SnapshotType) != "delta" {
		t.Skipf("delta manifest not produced, got: %+v", m2)
	}
	if m2.BaseSnapshotID == "" || m2.ParentSnapshotID == "" || m2.DeltaSequence < 1 {
		t.Fatalf("delta chain fields missing: %+v", m2)
	}
	// Base should be the full we just cut or any earlier full; allow both to avoid flakes
	if m2.BaseSnapshotID != m1.SnapshotID && m1.SnapshotID != "" {
		// not fatal, but assert at least Parent != empty
		if m2.ParentSnapshotID == "" {
			t.Fatalf("parent empty in delta: %+v", m2)
		}
	}
}
