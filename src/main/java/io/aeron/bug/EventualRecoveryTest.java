/*
 * Test demonstrating that after enough service messages are sent post-failover,
 * they eventually start going through again.
 *
 * This happens because nextServiceSessionId keeps incrementing with each cluster.offer()
 * call, and eventually exceeds logServiceSessionId, at which point the check passes
 * and messages are enqueued again.
 *
 * This proves the bug is a "silent drop window" rather than a permanent failure.
 */
package io.aeron.bug;

import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class EventualRecoveryTest {

    private static final int MSG_PING = 1;
    private static final int MSG_PONG = 2;

    private static final int NODE_COUNT = 3;
    private static final int PORT_BASE = 21000;
    private static final int MAX_PINGS_TO_SEND = 100;

    // Track which PONGs were received by followers (use Set to avoid counting duplicates from multiple followers)
    private static final Set<Integer> pongsReceivedByFollowers = new ConcurrentSkipListSet<>();
    private static final Set<Integer> pongsPublishedByLeader = new ConcurrentSkipListSet<>();
    private static final AtomicInteger currentLeader = new AtomicInteger(-1);

    // Buffer for node PONG reception messages (to print after PING status)
    private static final CopyOnWriteArrayList<String> pongReceptionBuffer = new CopyOnWriteArrayList<>();

    // Cluster nodes
    private static final ClusteredMediaDriver[] drivers = new ClusteredMediaDriver[NODE_COUNT];
    private static final ClusteredServiceContainer[] containers = new ClusteredServiceContainer[NODE_COUNT];
    private static final TestService[] services = new TestService[NODE_COUNT];

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("AERON CLUSTER BUG - EVENTUAL RECOVERY TEST");
        System.out.println("Demonstrates that messages eventually go through after enough attempts");
        System.out.println("=".repeat(80));
        System.out.println();

        try {
            // Step 1: Start 3-node cluster
            System.out.println("STEP 1: Starting 3-node cluster...");
            startCluster();
            System.out.println("  Waiting for leader election...");
            waitForLeader(30_000);
            int initialLeader = currentLeader.get();
            System.out.printf("Initial leader: Node %d%n%n", initialLeader);

            // Step 2: Send 10 PINGs before failover to establish baseline
            System.out.println("STEP 2: Sending 10 PINGs before failover (baseline)...");
            try (TestClient client = new TestClient()) {
                client.connect();

                pongsReceivedByFollowers.clear();
                pongsPublishedByLeader.clear();

                for (int i = 1; i <= 10; i++) {
                    pongReceptionBuffer.clear();
                    client.sendPing(i);
                    Thread.sleep(100);
                    client.poll();

                    boolean received = pongsReceivedByFollowers.contains(i);
                    System.out.printf("  PING #%d -> %s%n", i, received ? "PONG RECEIVED ✓" : "PONG DROPPED ✗");
                    flushPongBuffer();
                }

                Thread.sleep(500);
                client.poll();

                int successCount = pongsReceivedByFollowers.size();
                System.out.printf("%n  Before failover: %d/10 PONGs received%n", successCount);

                if (successCount == 10) {
                    System.out.println("  All messages going through - cluster is healthy!");
                }
            }

            // Step 3: Kill the leader
            int leaderToKill = initialLeader;
            System.out.printf("%nSTEP 3: Killing leader (Node %d)...%n", leaderToKill);
            killNode(leaderToKill);

            // Step 4: Wait for new leader
            System.out.println("STEP 4: Waiting for new leader election...");
            int newLeader = waitForNewLeader(leaderToKill, 30_000);
            System.out.printf("New leader elected: Node %d%n", newLeader);
            Thread.sleep(2000); // Stabilization time

            // Step 5: Keep sending PINGs until a PONG is received
            System.out.println("\nSTEP 5: Sending PINGs until a PONG is received...");
            System.out.println();

            pongsReceivedByFollowers.clear();
            pongsPublishedByLeader.clear();

            try (TestClient client = new TestClient(leaderToKill)) {
                client.connect();

                int pingNum = 0;
                int droppedCount = 0;

                while (pingNum < MAX_PINGS_TO_SEND) {
                    pingNum++;
                    pongReceptionBuffer.clear();
                    client.sendPing(pingNum);

                    // Brief pause and poll for responses
                    Thread.sleep(50);
                    client.poll();

                    // Check if this PONG was received
                    if (pongsReceivedByFollowers.contains(pingNum)) {
                        Thread.sleep(50); // Allow all nodes to add to buffer
                        System.out.printf("  PING #%d -> PONG RECEIVED! ✓%n", pingNum);
                        flushPongBuffer();
                        System.out.printf("%n  *** First successful PONG after %d dropped messages ***%n", droppedCount);
                        break;
                    } else {
                        droppedCount++;
                        // Print progress every message for first 10, then every 10th
                        if (pingNum <= 10 || pingNum % 10 == 0) {
                            System.out.printf("  PING #%d -> PONG DROPPED (waiting...)%n", pingNum);
                        }
                    }
                }

                // Final poll
                Thread.sleep(500);
                client.poll();
            }

            // Print results
            System.out.println();
            System.out.println("=".repeat(80));
            System.out.println("RESULTS:");
            System.out.println("=".repeat(80));

            int totalPublished = pongsPublishedByLeader.size();
            int totalReceived = pongsReceivedByFollowers.size();

            // Find the first successful PONG after failover
            int firstSuccess = -1;
            for (int i = 1; i <= totalPublished; i++) {
                if (pongsReceivedByFollowers.contains(i)) {
                    firstSuccess = i;
                    break;
                }
            }

            if (firstSuccess > 1) {
                System.out.println("*** BUG CONFIRMED WITH EVENTUAL RECOVERY ***");
                System.out.println();
                System.out.printf("  PINGs sent to get a response: %d%n", firstSuccess);
                System.out.printf("  Messages dropped: %d (PINGs #1 through #%d)%n", firstSuccess - 1, firstSuccess - 1);
                System.out.println();
                System.out.println("EXPLANATION:");
                System.out.println("  After failover, nextServiceSessionId < logServiceSessionId.");
                System.out.println("  Each cluster.offer() increments nextServiceSessionId by 1.");
                System.out.printf("  After %d attempts, nextServiceSessionId finally exceeds%n", firstSuccess);
                System.out.println("  logServiceSessionId, and messages start going through again.");
            } else if (firstSuccess == 1) {
                System.out.println("BUG NOT REPRODUCED - First PONG after failover was received.");
                System.out.println("(This can happen if a snapshot was taken, resetting the counters)");
            } else {
                System.out.println("ERROR: No PONGs received after " + totalPublished + " attempts.");
                System.out.println("The gap may be larger than MAX_PINGS_TO_SEND, or there's another issue.");
            }

        } finally {
            System.out.println("\nCleaning up...");
            cleanup();
        }
    }

    private static void startCluster() throws Exception {
        List<String> hostnames = List.of("localhost", "localhost", "localhost");

        for (int nodeId = 0; nodeId < NODE_COUNT; nodeId++) {
            String baseDir = "/tmp/aeron-recovery-test-" + nodeId;
            IoUtil.delete(new File(baseDir), true);

            services[nodeId] = new TestService(nodeId);

            ClusterConfig config = ClusterConfig.create(
                    nodeId,
                    hostnames,
                    hostnames,
                    PORT_BASE,
                    services[nodeId]
            );

            config.consensusModuleContext()
                    .ingressChannel("aeron:udp")
                    .errorHandler(t -> System.err.println("ConsensusModule error: " + t.getMessage()));
            config.baseDir(new File(baseDir));

            drivers[nodeId] = ClusteredMediaDriver.launch(
                    config.mediaDriverContext(),
                    config.archiveContext(),
                    config.consensusModuleContext()
            );

            containers[nodeId] = ClusteredServiceContainer.launch(
                    config.clusteredServiceContext()
            );

            System.out.printf("  Node %d started%n", nodeId);
            Thread.sleep(500);
        }
    }

    private static void waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < NODE_COUNT; i++) {
                if (services[i] != null && services[i].isLeader()) {
                    currentLeader.set(i);
                    return;
                }
            }
            Thread.sleep(100);
        }
        throw new RuntimeException("No leader elected within timeout");
    }

    private static int waitForNewLeader(int excludeNode, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < NODE_COUNT; i++) {
                if (i != excludeNode && services[i] != null && services[i].isLeader()) {
                    currentLeader.set(i);
                    return i;
                }
            }
            Thread.sleep(100);
        }
        throw new RuntimeException("No new leader elected within timeout");
    }

    private static void killNode(int nodeId) {
        CloseHelper.quietClose(containers[nodeId]);
        CloseHelper.quietClose(drivers[nodeId]);
        containers[nodeId] = null;
        drivers[nodeId] = null;
        services[nodeId] = null;
    }

    private static void cleanup() {
        for (int i = NODE_COUNT - 1; i >= 0; i--) {
            CloseHelper.quietClose(containers[i]);
            CloseHelper.quietClose(drivers[i]);
        }
        for (int i = 0; i < NODE_COUNT; i++) {
            IoUtil.delete(new File("/tmp/aeron-recovery-test-" + i), true);
        }
    }

    private static void flushPongBuffer() {
        for (String msg : pongReceptionBuffer) {
            System.out.println(msg);
        }
        pongReceptionBuffer.clear();
    }

    // ==================== Test Service ====================

    static class TestService implements ClusteredService {
        private final int nodeId;
        private Cluster cluster;
        private final AtomicBoolean isLeader = new AtomicBoolean(false);
        private final ExpandableArrayBuffer sendBuffer = new ExpandableArrayBuffer(64);

        TestService(int nodeId) {
            this.nodeId = nodeId;
        }

        boolean isLeader() {
            return isLeader.get();
        }

        @Override
        public void onStart(Cluster cluster, Image snapshotImage) {
            this.cluster = cluster;
        }

        @Override
        public void onSessionOpen(ClientSession session, long timestamp) {}

        @Override
        public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {}

        @Override
        public void onSessionMessage(ClientSession session, long timestamp,
                                      DirectBuffer buffer, int offset, int length, Header header) {
            int templateId = buffer.getInt(offset);

            if (templateId == MSG_PING) {
                int pingNumber = buffer.getInt(offset + 4);

                if (cluster.role() == Cluster.Role.LEADER) {
                    // Publish PONG via cluster.offer()
                    sendBuffer.putInt(0, MSG_PONG);
                    sendBuffer.putInt(4, pingNumber);
                    sendBuffer.putLong(8, System.nanoTime());

                    long result = cluster.offer(sendBuffer, 0, 16);
                    pongsPublishedByLeader.add(pingNumber);
                }

            } else if (templateId == MSG_PONG) {
                int pingNumber = buffer.getInt(offset + 4);

                // Buffer the message to print after PING status
                pongReceptionBuffer.add(String.format("    [Node %d, role=%s] received PONG #%d",
                        nodeId, cluster.role(), pingNumber));

                // Count PONGs received by followers (used for bug detection)
                if (cluster.role() != Cluster.Role.LEADER) {
                    pongsReceivedByFollowers.add(pingNumber);
                }
            }
        }

        @Override
        public void onTimerEvent(long correlationId, long timestamp) {}

        @Override
        public void onTakeSnapshot(ExclusivePublication snapshotPublication) {}

        @Override
        public void onRoleChange(Cluster.Role newRole) {
            isLeader.set(newRole == Cluster.Role.LEADER);
            if (newRole == Cluster.Role.LEADER) {
                currentLeader.set(nodeId);
            }
        }

        @Override
        public void onTerminate(Cluster cluster) {}

        @Override
        public void onNewLeadershipTermEvent(long leadershipTermId, long logPosition, long timestamp,
                                              long termBaseLogPosition, int leaderMemberId, int logSessionId,
                                              TimeUnit timeUnit, int appVersion) {}
    }

    // ==================== Test Client ====================

    static class TestClient implements EgressListener, AutoCloseable {
        private MediaDriver driver;
        private AeronCluster cluster;
        private final ExpandableArrayBuffer sendBuffer = new ExpandableArrayBuffer(64);
        private final int excludeNode;

        TestClient() {
            this(-1);
        }

        TestClient(int excludeNode) {
            this.excludeNode = excludeNode;
        }

        void connect() throws InterruptedException {
            Thread.sleep(2000);

            String aeronDir = CommonContext.generateRandomDirName();

            StringBuilder ingressEndpoints = new StringBuilder();
            boolean first = true;
            for (int i = 0; i < NODE_COUNT; i++) {
                if (i == excludeNode) continue;
                if (!first) ingressEndpoints.append(",");
                first = false;
                int ingressPort = PORT_BASE + (i * 100) + 2;
                ingressEndpoints.append(i).append("=localhost:").append(ingressPort);
            }

            driver = MediaDriver.launchEmbedded(
                    new MediaDriver.Context()
                            .aeronDirectoryName(aeronDir)
                            .threadingMode(io.aeron.driver.ThreadingMode.SHARED)
                            .dirDeleteOnStart(true)
                            .dirDeleteOnShutdown(true)
            );

            cluster = AeronCluster.connect(
                    new AeronCluster.Context()
                            .aeronDirectoryName(driver.aeronDirectoryName())
                            .egressListener(this)
                            .ingressChannel("aeron:udp")
                            .egressChannel("aeron:udp?endpoint=localhost:0")
                            .ingressEndpoints(ingressEndpoints.toString())
                            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(10))
            );
        }

        void sendPing(int pingNumber) {
            sendBuffer.putInt(0, MSG_PING);
            sendBuffer.putInt(4, pingNumber);

            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1000, 1_000_000);
            while (cluster.offer(sendBuffer, 0, 8) < 0) {
                idle.idle();
            }
        }

        void poll() {
            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1000, 1_000_000);
            long deadline = System.currentTimeMillis() + 500;
            while (System.currentTimeMillis() < deadline) {
                cluster.pollEgress();
                idle.idle();
            }
        }

        @Override
        public void onMessage(long clusterSessionId, long timestamp,
                              DirectBuffer buffer, int offset, int length, Header header) {}

        @Override
        public void onSessionEvent(long correlationId, long clusterSessionId,
                                    long leadershipTermId, int leaderMemberId,
                                    EventCode code, String detail) {}

        @Override
        public void onNewLeader(long clusterSessionId, long leadershipTermId,
                                int leaderMemberId, String ingressEndpoints) {}

        @Override
        public void close() {
            CloseHelper.quietClose(cluster);
            CloseHelper.quietClose(driver);
        }
    }
}
