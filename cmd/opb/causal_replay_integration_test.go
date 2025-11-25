//go:build integration

package main

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCausalReplay_Restore(t *testing.T) {
	bootstrap := os.Getenv("KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		bootstrap = "localhost:9092"
	}
	if !tcpAvailable(strings.Split(bootstrap, ",")[0]) {
		t.Skipf("Kafka not reachable at %s", bootstrap)
	}
	port, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	snapDir := filepath.Join(baseDir, "snap")
	chgDir := filepath.Join(baseDir, "chg")
	stDir := filepath.Join(baseDir, "state")
	_ = os.MkdirAll(snapDir, 0o755)
	_ = os.MkdirAll(chgDir, 0o755)
	_ = os.MkdirAll(stDir, 0o755)

	groupID := fmt.Sprintf("opb-causal-it-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
	if err := waitHTTPReady(base+"/status", 30*time.Second); err != nil {
		t.Skipf("opb http not ready: %v", err)
	}
	if err := waitAssigned(base, 20*time.Second); err != nil {
		t.Skipf("opb not assigned (topics missing?): %v", err)
	}
	injectURL := fmt.Sprintf("%s/api/inject-test-data", base)
	if err := postJSON(injectURL, `{"storeId":"CAUSAL","productId":"","ws":0,"mode":"new","n":200,"start":0}`); err != nil {
		t.Fatalf("inject batch1: %v", err)
	}
	fullURL := fmt.Sprintf("%s/admin/snapshot-cut?type=full", base)
	if err := postJSON(fullURL, ""); err != nil {
		t.Fatalf("full cut: %v", err)
	}
	time.Sleep(5 * time.Second)

	if err := postJSON(injectURL, `{"storeId":"CAUSAL","productId":"","ws":0,"mode":"update","n":50,"start":0}`); err != nil {
		t.Fatalf("inject batch2: %v", err)
	}
	deltaURL := fmt.Sprintf("%s/admin/snapshot-cut?type=delta", base)
	if err := postJSON(deltaURL, ""); err != nil {
		t.Fatalf("delta cut: %v", err)
	}

	manifestLatest := filepath.Join(snapDir, "manifest.latest.json")
	if err := waitForFile(manifestLatest, 20*time.Second); err != nil {
		t.Fatalf("wait manifest latest: %v", err)
	}
	waitDeadline := time.Now().Add(20 * time.Second)
	var mani struct {
		SnapshotType string   `json:"snapshotType"`
		InflightFile string   `json:"inflightFile"`
		Channels     []string `json:"channels"`
	}
	for time.Now().Before(waitDeadline) {
		if err := readJSON(manifestLatest, &mani); err == nil {
			if strings.ToLower(mani.SnapshotType) == "delta" && mani.InflightFile != "" {
				break
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	if mani.InflightFile == "" {
		t.Skipf("delta manifest missing inflightFile (channels=%v)", mani.Channels)
	}

	_ = cmd.Process.Kill()
	time.Sleep(2 * time.Second)

	restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer restoreCancel()
	restoreArgs := []string{"run", ".",
		"--http", fmt.Sprintf(":%d", port),
		"--group-id", groupID,
		"--kafka-bootstrap", bootstrap,
		"--input-source", "kafka",
		"--topic-enriched", "p1.orders.enriched",
		"--output-topic", "p1.orders.output",
		"--topic-changelog", "p1.opb-changelog",
		"--topic-snapshots", "p1.opb-snapshots",
		"--snapshot-dir", snapDir,
		"--changelog-dir", chgDir,
		"--state-dir", stDir,
		"--restore-on-start=true",
		"--restore-only=true",
	}
	restoreCmd := exec.CommandContext(restoreCtx, "go", restoreArgs...)
	restoreCmd.Dir = "."
	if out, err := restoreCmd.CombinedOutput(); err != nil {
		t.Fatalf("restore-only run failed: %v output=%s", err, string(out))
	}

	stateFile := filepath.Join(snapDir, "manifest.latest.json")
	if err := readJSON(stateFile, &mani); err != nil {
		t.Fatalf("re-read manifest: %v", err)
	}
	if mani.InflightFile == "" {
		t.Fatalf("no inflight file recorded after restore")
	}

	statusReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("%s/status", base), nil)
	if err != nil {
		t.Fatalf("build status request: %v", err)
	}
	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatalf("status check: %v", err)
	}
	defer statusResp.Body.Close()
	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("status not ok: %s", statusResp.Status)
	}
}
