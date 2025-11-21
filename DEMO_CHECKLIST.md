# OpB Enhanced Observability & Testability - Demo Checklist

This checklist demonstrates the new features for OpB observability, testability, and user experience.

## Prerequisites
- Kafka running on `localhost:9092`
- OpB compiled: `go build -o ./bin/opb ./cmd/opb`
- OpB running: `./bin/opb -kafka-bootstrap 127.0.0.1:9092 -state-backend memory -http :8089 -instance-id B1`

## Demo Flow

### 1. Heatmap Visualization
- [ ] Open `http://localhost:8089/viz/` in browser
- [ ] Verify heatmap displays aggregated data (sumQty or sumAmount per store)
- [ ] Hover over cells to see tooltip with `lastUpdatedBy` (instance-id)
- [ ] Click on a cell to navigate to zone details page

**Expected**: Heatmap shows color-coded cells, tooltips show instance IDs, cells are clickable.

### 2. Zone Details Page
- [ ] Navigate to `http://localhost:8089/viz/zone.html?id=A-` (or any store ID)
- [ ] Verify displays:
  - `sumQty` and `sumAmount` for the store
  - `lastSeq` (last sequence number)
  - `lastUpdatedBy` (instance-id that last updated this store)
  - `relatedInstances` (list of instance-ids that have contributed)
  - EOS counters (`applied`, `skipped_dedup`, `skipped_seq`)
- [ ] Verify page auto-refreshes every 2 seconds

**Expected**: Zone page shows complete store details with instance visibility and EOS metrics.

### 3. Test Data Injection
- [ ] On zone page, use "Inject New Data" button:
  - Store ID: `A-`
  - Number of events: `100`
  - Click "Inject"
- [ ] Observe zone page updates after 2-3 seconds
- [ ] Verify `sumQty` and `sumAmount` increase
- [ ] Verify `applied` counter increases

**Expected**: New events are injected and processed, aggregates update.

### 4. Duplicate Injection (EOS Proof)
- [ ] Note current `sumQty` value on zone page
- [ ] Use "Inject Duplicate Data" button:
  - Store ID: `A-`
  - Number of events: `100`
  - Click "Inject"
- [ ] Observe zone page updates after 2-3 seconds
- [ ] Verify `sumQty` **does NOT change** (EOS semantics)
- [ ] Verify `skipped_dedup` or `skipped_seq` counter increases

**Expected**: Duplicates are skipped, state remains stable, skipped counters increase.

### 5. Scale-Out Visualization
- [ ] Start second OpB instance: 
  ```bash
  ./bin/opb -kafka-bootstrap 127.0.0.1:9092 -state-backend memory -http :8090 -instance-id B2
  ```
- [ ] Inject some data (new or duplicate) via either instance
- [ ] Navigate to zone details page for a store that received updates
- [ ] Verify `relatedInstances` shows **both** `B1` and `B2`
- [ ] Verify `lastUpdatedBy` shows the instance that processed the most recent event
- [ ] Check heatmap tooltips - they should show different instance IDs for different stores

**Expected**: Both instances are visible in zone details, demonstrating scale-out capability.

### 6. Status Endpoint
- [ ] Query `http://localhost:8089/status`
- [ ] Verify JSON response includes:
  - `status`: "starting" or "healthy"
  - `instance`: instance ID
  - `groupId`: consumer group ID
  - `windowSizeSec`: window size configuration

**Expected**: Status endpoint provides machine-readable instance state.

### 7. Prometheus Metrics
- [ ] Query `http://localhost:8089/metrics`
- [ ] Verify presence of:
  - `opb_events_applied_total`: total events applied
  - `opb_events_skipped_dedup_total`: events skipped due to in-process deduplication
  - `opb_events_skipped_seq_total`: events skipped due to sequence number check

**Expected**: All EOS-related metrics are exported for Prometheus scraping.

### 8. API Endpoints (Programmatic Access)

#### Zone Details API
```bash
curl http://localhost:8089/api/zone-details?id=A-
```
- [ ] Verify returns JSON with store aggregates, instance info, and EOS counters

#### Inject Test Data API
```bash
curl -X POST http://localhost:8089/api/inject-test-data \
  -H "Content-Type: application/json" \
  -d '{"storeId":"A-","mode":"new","n":50}'
```
- [ ] Verify returns `{"status":"queued",...}`
- [ ] Verify events are processed and visible in zone details

#### Rate Limiting Test
```bash
# Send two requests quickly (< 2s apart)
curl -X POST http://localhost:8089/api/inject-test-data -d '{"storeId":"A-","n":10}'
curl -X POST http://localhost:8089/api/inject-test-data -d '{"storeId":"A-","n":10}'
```
- [ ] Verify second request returns `429 Too Many Requests` (rate limited)

**Expected**: API endpoints work correctly, rate limiting prevents abuse.

### 9. Integration Test Suite
- [ ] Run integration suite: `./scripts/run_integration_suite.sh`
- [ ] Verify all three scenarios pass:
  1. **EOS Proof**: Duplicates keep state constant, counters increase
  2. **Scale-Out**: Multiple instances visible in zone details
  3. **Recovery**: TTR < 10s, state restored correctly

**Expected**: All integration tests pass, demonstrating end-to-end correctness.

## Quick Verification Commands

```bash
# Check zone details
curl http://localhost:8089/api/zone-details?id=A- | grep -E "(sumQty|sumAmount|instances)"

# Check EOS metrics
curl http://localhost:8089/metrics | grep -E "opb_events_(applied|skipped)"

# Inject test data
curl -X POST http://localhost:8089/api/inject-test-data \
  -H "Content-Type: application/json" \
  -d '{"storeId":"TEST-","mode":"new","n":10}'

# Check instance status
curl http://localhost:8089/status
```

## Success Criteria

✅ Heatmap visualizes aggregated data with instance visibility  
✅ Zone details page shows complete store information  
✅ Test data injection works (new and duplicate modes)  
✅ EOS semantics verified: duplicates don't change state  
✅ Scale-out visible: multiple instances contribute to aggregates  
✅ Status endpoint provides machine-readable state  
✅ Prometheus metrics exported for observability  
✅ Rate limiting prevents API abuse  
✅ Integration tests pass end-to-end

