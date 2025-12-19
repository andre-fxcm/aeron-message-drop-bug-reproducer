# Aeron Cluster Service Message Drop Bug Reproducer

**Issue:** https://github.com/aeron-io/aeron/issues/1903

## Bug Summary

After a leadership transition (failover) in an Aeron Cluster, service-initiated messages via `Cluster.offer()` are **silently dropped** despite the method returning a successful position value.

## Root Cause

In `PendingServiceMessageTracker.sweepFollowerMessages()`:

```java
void sweepFollowerMessages(final long clusterSessionId)
{
    logServiceSessionId = clusterSessionId;  // ✓ UPDATED
    // nextServiceSessionId is NOT updated!   // ✗ BUG
    pendingMessages.consume(followerMessageSweeper, Integer.MAX_VALUE);
}
```

When `enqueueMessage()` is called later:

```java
void enqueueMessage(...)
{
    final long clusterSessionId = nextServiceSessionId++;
    if (clusterSessionId > logServiceSessionId)  // ← FAILS after failover!
    {
        // enqueue
    }
    // else: SILENTLY DROPPED - no error, no exception, no logging
}
```

## The Fix (One Line)

```java
void sweepFollowerMessages(final long clusterSessionId)
{
    logServiceSessionId = clusterSessionId;
    nextServiceSessionId = clusterSessionId + 1;  // ← ADD THIS LINE
    pendingMessages.consume(followerMessageSweeper, Integer.MAX_VALUE);
}
```

## Running the Reproducer

### Prerequisites
- Java 21+
- Maven 3.8+

### Build

```bash
cd aeron-message-drop-bug-reproducer
mvn clean compile
```

### Run Automated Reproducer (Recommended)

This runs the full scenario automatically:

```bash
mvn exec:exec -Dexec.mainClass="io.aeron.bug.AutomatedBugReproducer"
```

> **Note:** We use `exec:exec` instead of `exec:java` because Aeron/Agrona requires `--add-opens` JVM arguments to access internal JDK APIs on Java 9+. These are configured in `pom.xml`.

Expected output:

```
================================================================================
AERON CLUSTER BUG REPRODUCER
Service messages silently dropped after leadership transition
https://github.com/aeron-io/aeron/issues/1903
================================================================================

STEP 1: Starting 3-node cluster...
  Node 0 started
  Node 1 started
  Node 2 started
    [Node 1] Role changed to LEADER
  Waiting for leader election...
Initial leader: Node 1

STEP 2: Sending PING before failover...
  Ingress endpoints: 0=localhost:20002,1=localhost:20102,2=localhost:20202
  Client connected to leader: 1
  Client sent PING #1
    [Node 1] Received PING #1 (role=LEADER)
    [Node 1] Published PONG #1, cluster.offer() = 288
    [Node 2] FOLLOWER received PONG #1
    [Node 0] FOLLOWER received PONG #1
Before failover: Leader published 1 PONGs, Followers received 2 PONGs

STEP 3: Killing leader (Node 1)...
STEP 4: Waiting for new leader election...
    [Node 0] Role changed to LEADER
New leader elected: Node 0

STEP 5: Sending PING after failover (BUG WILL MANIFEST HERE)...
  Ingress endpoints: 0=localhost:20002,2=localhost:20202
  Client connected to leader: 0
  Client sent PING #2
    [Node 0] Received PING #2 (role=LEADER)
    [Node 0] Published PONG #2, cluster.offer() = 384
    [Node 2] Received PING #2 (role=FOLLOWER)

================================================================================
RESULTS:
================================================================================
Leader published 1 PONGs (cluster.offer() returned: 384)
Followers received 0 PONGs

*** BUG CONFIRMED! ***
Leader's cluster.offer() returned success (384)
But PONG was NEVER received by followers!

ROOT CAUSE:
  PendingServiceMessageTracker.sweepFollowerMessages() updates
  logServiceSessionId but NOT nextServiceSessionId.
  The check (nextServiceSessionId > logServiceSessionId) fails,
  causing the message to be silently discarded.

FIX:
  In sweepFollowerMessages(), add:
    nextServiceSessionId = clusterSessionId + 1;
```

### Manual Testing (Alternative)

**Terminal 1 - Node 0:**
```bash
mvn exec:exec -Dexec.mainClass="io.aeron.bug.ServiceMessageDropReproducer" -Dnode.id=0
```

**Terminal 2 - Node 1:**
```bash
mvn exec:exec -Dexec.mainClass="io.aeron.bug.ServiceMessageDropReproducer" -Dnode.id=1
```

**Terminal 3 - Node 2:**
```bash
mvn exec:exec -Dexec.mainClass="io.aeron.bug.ServiceMessageDropReproducer" -Dnode.id=2
```

**Terminal 4 - Client:**
```bash
mvn exec:exec -Dexec.mainClass="io.aeron.bug.TestClient"
```

Then:
1. Type `1` and press Enter → Should see PONG received
2. Kill the leader terminal (Ctrl+C)
3. Wait for new leader election
4. Type `2` and press Enter → **BUG: PONG never received despite cluster.offer() success!**

## Impact

- **Severity:** Critical (Data Loss)
- **Silent Failure:** No indication to the application that the message was dropped
- **Misleading Return Value:** `cluster.offer()` returns positive position, suggesting success
- **State Inconsistency:** Leader and followers have different state

## Affected Versions

Tested with Aeron 1.46.x, but the bug has likely existed for many versions.

## Files

- `PendingServiceMessageTracker.java` - Lines 87-105 (`enqueueMessage`)
- `PendingServiceMessageTracker.java` - Lines 107-111 (`sweepFollowerMessages`)
