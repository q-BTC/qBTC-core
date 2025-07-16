# Peer List Synchronization Fix Summary

## Problem
The connected peers list (`dht_peers`) was growing infinitely while the synced peers list (`synced_peers`) remained accurate. This was because:
- Peers were added to both lists when discovered
- Failed peers were removed from `synced_peers` but not from `dht_peers`
- The failure threshold for removing peers from `dht_peers` was too high (20 failures)

## Solution Implemented

### 1. **Reduced Failure Thresholds**
- Reduced peer removal threshold from 20 failures to 5 failures in `check_partition()`
- Reduced message send failure threshold from 10 to 5 in `_send_message()`
- Added immediate removal from `synced_peers` after 3 failures in `periodic_sync()`

### 2. **Added Periodic Cleanup Task**
- New `periodic_cleanup()` method runs every 2 minutes
- Removes peers from `dht_peers` that:
  - Are not in `synced_peers`
  - Are not protected peers
  - Have been unsynced for more than 3 minutes
- Ensures consistency between both peer lists

### 3. **Enhanced Stale Peer Detection**
- Added age-based cleanup in `check_partition()`
- Peers unsynced for >5 minutes are removed
- Timestamp tracking for all peers to enable age-based cleanup

### 4. **Protected Peers**
- Protected peers are never removed regardless of failures
- This ensures critical network infrastructure peers remain connected

## Files Modified
- `/home/qBTC-core/gossip/gossip.py`:
  - Added `periodic_cleanup()` method
  - Modified `check_partition()` to be more aggressive
  - Updated `_send_message()` failure handling
  - Enhanced `periodic_sync()` with failure tracking
  - Added `cleanup_task` initialization and cancellation

## Expected Behavior After Fix
- `dht_peers` (connected peers) should closely match `synced_peers`
- Stale/failed peers are removed within 3-5 minutes
- Protected peers remain connected regardless of failures
- Both lists stay synchronized through periodic cleanup

## Testing
To verify the fix:
1. Monitor the peer counts in the web interface or API
2. Check that connected peers ≈ synced peers
3. Verify that failed peers are removed within 5 minutes
4. Ensure protected peers are never removed