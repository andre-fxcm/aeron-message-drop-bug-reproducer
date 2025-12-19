/*
 * Minimal reproducer for Aeron Cluster bug:
 * Service messages via cluster.offer() are silently dropped after leadership transition.
 *
 * Bug: PendingServiceMessageTracker.sweepFollowerMessages() updates logServiceSessionId
 *      but does NOT update nextServiceSessionId. After failover, the session ID check
 *      fails and messages are silently discarded.
 *
 * See: https://github.com/aeron-io/aeron/issues/1903
 */
package io.aeron.bug;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A minimal clustered service that demonstrates the message drop bug.
 *
 * The service:
 * 1. Receives a PING message (templateId=1) from client ingress
 * 2. Publishes a PONG message (templateId=2) via cluster.offer()
 * 3. When PONG comes back through consensus, increments a counter
 *
 * BUG BEHAVIOR:
 * - Before failover: PONG messages are received by all nodes
 * - After failover: PONG messages are silently dropped despite cluster.offer() returning success
 */
public class ServiceMessageDropReproducer implements ClusteredService {

    private static final int MSG_PING = 1;
    private static final int MSG_PONG = 2;

    private Cluster cluster;
    private final int nodeId;

    // Counters to track message flow
    private final AtomicInteger pingsReceived = new AtomicInteger(0);
    private final AtomicInteger pongsPublished = new AtomicInteger(0);
    private final AtomicInteger pongsReceived = new AtomicInteger(0);
    private final AtomicInteger pongsDropped = new AtomicInteger(0);

    // Track the last cluster.offer() result
    private final AtomicLong lastOfferResult = new AtomicLong(0);

    private final ExpandableArrayBuffer sendBuffer = new ExpandableArrayBuffer(64);

    public ServiceMessageDropReproducer(int nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        System.out.printf("[Node %d] Service started, role=%s%n", nodeId, cluster.role());
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        System.out.printf("[Node %d] Client session opened: %d%n", nodeId, session.id());
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp,
                               io.aeron.cluster.codecs.CloseReason closeReason) {
        System.out.printf("[Node %d] Client session closed: %d%n", nodeId, session.id());
    }

    @Override
    public void onSessionMessage(ClientSession session, long timestamp,
                                  DirectBuffer buffer, int offset, int length,
                                  Header header) {
        int templateId = buffer.getInt(offset);

        if (templateId == MSG_PING) {
            handlePing(buffer, offset, length);
        } else if (templateId == MSG_PONG) {
            handlePong(buffer, offset, length);
        }
    }

    private void handlePing(DirectBuffer buffer, int offset, int length) {
        int pingNumber = buffer.getInt(offset + 4);
        pingsReceived.incrementAndGet();

        System.out.printf("[Node %d] Received PING #%d (role=%s)%n",
                nodeId, pingNumber, cluster.role());

        // Only leader processes and publishes PONG
        if (cluster.role() == Cluster.Role.LEADER) {
            publishPong(pingNumber);
        }
    }

    private void publishPong(int pingNumber) {
        // Encode PONG message
        sendBuffer.putInt(0, MSG_PONG);       // templateId
        sendBuffer.putInt(4, pingNumber);     // echo the ping number
        sendBuffer.putLong(8, System.nanoTime()); // timestamp

        // Publish via cluster.offer() - THIS IS WHERE THE BUG MANIFESTS
        long result = cluster.offer(sendBuffer, 0, 16);

        lastOfferResult.set(result);
        pongsPublished.incrementAndGet();

        if (result > 0) {
            // Result > 0 means IPC publication succeeded
            // But this does NOT mean the message will be in the consensus log!
            System.out.printf("[Node %d] Published PONG #%d, cluster.offer() returned %d (IPC position)%n",
                    nodeId, pingNumber, result);
        } else {
            System.out.printf("[Node %d] PONG #%d cluster.offer() FAILED with %d%n",
                    nodeId, pingNumber, result);
        }
    }

    private void handlePong(DirectBuffer buffer, int offset, int length) {
        int pingNumber = buffer.getInt(offset + 4);
        pongsReceived.incrementAndGet();

        System.out.printf("[Node %d] Received PONG #%d via consensus log (role=%s)%n",
                nodeId, pingNumber, cluster.role());
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        // Not used in this reproducer
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        System.out.printf("[Node %d] Taking snapshot%n", nodeId);
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        System.out.printf("[Node %d] *** ROLE CHANGE: %s ***%n", nodeId, newRole);

        if (newRole == Cluster.Role.LEADER) {
            System.out.printf("[Node %d] Became LEADER - stats: pings=%d, pongsPublished=%d, pongsReceived=%d%n",
                    nodeId, pingsReceived.get(), pongsPublished.get(), pongsReceived.get());
        }
    }

    @Override
    public void onTerminate(Cluster cluster) {
        System.out.printf("[Node %d] Service terminating. Final stats:%n", nodeId);
        System.out.printf("  Pings received: %d%n", pingsReceived.get());
        System.out.printf("  PONGs published: %d%n", pongsPublished.get());
        System.out.printf("  PONGs received: %d%n", pongsReceived.get());
        System.out.printf("  Last offer result: %d%n", lastOfferResult.get());

        if (pongsPublished.get() > pongsReceived.get()) {
            int dropped = pongsPublished.get() - pongsReceived.get();
            System.out.printf("  *** BUG DETECTED: %d PONGs were SILENTLY DROPPED! ***%n", dropped);
        }
    }

    @Override
    public void onNewLeadershipTermEvent(
            long leadershipTermId, long logPosition, long timestamp,
            long termBaseLogPosition, int leaderMemberId, int logSessionId,
            TimeUnit timeUnit, int appVersion) {
        System.out.printf("[Node %d] New leadership term: termId=%d, leader=%d%n",
                nodeId, leadershipTermId, leaderMemberId);
    }

    // ==================== Cluster Node Launcher ====================

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ServiceMessageDropReproducer <nodeId>");
            System.err.println("  nodeId: 0, 1, or 2");
            System.exit(1);
        }

        int nodeId = Integer.parseInt(args[0]);
        List<String> hostnames = List.of("localhost", "localhost", "localhost");
        int portBase = 9000;  // Same port base for all nodes - ClusterConfig handles per-node offsets

        System.out.printf("Starting node %d with portBase %d%n", nodeId, portBase);

        // Clean up any previous state
        String baseDir = "/tmp/aeron-cluster-bug-" + nodeId;
        deleteDirectory(new File(baseDir));

        // Create cluster configuration using Aeron's sample helper
        ClusterConfig config = ClusterConfig.create(
                nodeId,
                hostnames,
                hostnames,
                portBase,
                new ServiceMessageDropReproducer(nodeId)
        );

        config.consensusModuleContext().ingressChannel("aeron:udp");
        config.baseDir(new File(baseDir));

        ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        try (ClusteredMediaDriver driver = ClusteredMediaDriver.launch(
                     config.mediaDriverContext(),
                     config.archiveContext(),
                     config.consensusModuleContext());
             ClusteredServiceContainer container = ClusteredServiceContainer.launch(
                     config.clusteredServiceContext())) {

            System.out.printf("[Node %d] Cluster node started. Press Ctrl+C to stop.%n", nodeId);
            barrier.await();
        }
    }

    private static void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
