/*
 * Automated reproducer for Aeron Cluster service message drop bug.
 * This runs the full scenario without manual intervention.
 *
 * BUG: After leadership transition, service messages via cluster.offer()
 *      are silently dropped despite returning a successful position value.
 *
 * ROOT CAUSE: PendingServiceMessageTracker.sweepFollowerMessages() updates
 *             logServiceSessionId but NOT nextServiceSessionId.
 *
 * FIX: In sweepFollowerMessages(), add: nextServiceSessionId = clusterSessionId + 1;
 */
package io.aeron.bug;

import io.aeron.Aeron;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AutomatedBugReproducer {

    private static final int MSG_PING = 1;
    private static final int MSG_PONG = 2;

    private static final int NODE_COUNT = 3;
    private static final int PORT_BASE = 20000;

    // Track cluster state
    private static final AtomicInteger currentLeader = new AtomicInteger(-1);
    private static final AtomicInteger pongsReceivedByFollowers = new AtomicInteger(0);
    private static final AtomicInteger pongsPublishedByLeader = new AtomicInteger(0);
    private static final AtomicLong lastOfferResult = new AtomicLong(0);

    // Cluster nodes
    private static final ClusteredMediaDriver[] drivers = new ClusteredMediaDriver[NODE_COUNT];
    private static final ClusteredServiceContainer[] containers = new ClusteredServiceContainer[NODE_COUNT];
    private static final TestService[] services = new TestService[NODE_COUNT];

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("AERON CLUSTER BUG REPRODUCER");
        System.out.println("Service messages silently dropped after leadership transition");
        System.out.println("https://github.com/aeron-io/aeron/issues/1903");
        System.out.println("=".repeat(80));
        System.out.println();

        try {
            // Step 1: Start 3-node cluster
            System.out.println("STEP 1: Starting 3-node cluster...");
            startCluster();
            System.out.println("  Waiting for leader election...");
            waitForLeader(30_000);  // 30 second timeout for leader election
            System.out.printf("Initial leader: Node %d%n%n", currentLeader.get());

            // Step 2: Connect client and send PING before failover
            System.out.println("STEP 2: Sending PING before failover...");
            int leaderToKill;
            try (TestClient client = new TestClient()) {
                client.connect();
                leaderToKill = client.getLeaderId();

                // Send PING and wait for PONG
                pongsReceivedByFollowers.set(0);
                pongsPublishedByLeader.set(0);
                client.sendPing(1);
                Thread.sleep(1000);
                client.poll();

                System.out.printf("Before failover: Leader published %d PONGs, Followers received %d PONGs%n",
                        pongsPublishedByLeader.get(), pongsReceivedByFollowers.get());

                if (pongsPublishedByLeader.get() == 0) {
                    System.out.println("ERROR: No PONGs published. Check cluster setup.");
                    return;
                }

                // Verify followers received the PONG
                if (pongsReceivedByFollowers.get() < (NODE_COUNT - 1)) {
                    System.out.println("WARNING: Not all followers received PONG before failover");
                }
            }
            // Client closed here

            // Step 3: Kill the leader
            System.out.printf("%nSTEP 3: Killing leader (Node %d)...%n", leaderToKill);
            killNode(leaderToKill);

            // Step 4: Wait for new leader election
            System.out.println("STEP 4: Waiting for new leader election...");
            // Wait until a new leader is elected
            long deadline = System.currentTimeMillis() + 30_000;
            int newLeaderId = -1;
            while (System.currentTimeMillis() < deadline) {
                for (int i = 0; i < NODE_COUNT; i++) {
                    if (i != leaderToKill && services[i] != null && services[i].isLeader()) {
                        newLeaderId = i;
                        break;
                    }
                }
                if (newLeaderId >= 0) break;
                Thread.sleep(100);
            }
            if (newLeaderId < 0) {
                System.out.println("ERROR: No new leader elected within timeout");
                return;
            }
            System.out.printf("New leader elected: Node %d%n", newLeaderId);
            Thread.sleep(2000); // Extra stabilization time

            // Step 5: Create fresh client - exclude killed node
            System.out.println("STEP 5: Sending PING after failover (BUG WILL MANIFEST HERE)...");
            try (TestClient client = new TestClient(leaderToKill)) {
                client.connect();
                int newLeader = client.getLeaderId();
                System.out.printf("New leader: Node %d%n%n", newLeader);

                pongsReceivedByFollowers.set(0);
                pongsPublishedByLeader.set(0);

                client.sendPing(2);
                Thread.sleep(2000);
                client.poll();

                System.out.println();
                System.out.println("=".repeat(80));
                System.out.println("RESULTS:");
                System.out.println("=".repeat(80));
                System.out.printf("Leader published %d PONGs (cluster.offer() returned: %d)%n",
                        pongsPublishedByLeader.get(), lastOfferResult.get());
                System.out.printf("Followers received %d PONGs%n", pongsReceivedByFollowers.get());

                if (pongsPublishedByLeader.get() > 0 && pongsReceivedByFollowers.get() == 0) {
                    System.out.println();
                    System.out.println("*** BUG CONFIRMED! ***");
                    System.out.println("Leader's cluster.offer() returned success (" + lastOfferResult.get() + ")");
                    System.out.println("But PONG was NEVER received by followers!");
                    System.out.println();
                    System.out.println("ROOT CAUSE:");
                    System.out.println("  PendingServiceMessageTracker.sweepFollowerMessages() updates");
                    System.out.println("  logServiceSessionId but NOT nextServiceSessionId.");
                    System.out.println("  The check (nextServiceSessionId > logServiceSessionId) fails,");
                    System.out.println("  causing the message to be silently discarded.");
                    System.out.println();
                    System.out.println("FIX:");
                    System.out.println("  In sweepFollowerMessages(), add:");
                    System.out.println("    nextServiceSessionId = clusterSessionId + 1;");
                } else if (pongsReceivedByFollowers.get() > 0) {
                    System.out.println();
                    System.out.println("BUG NOT REPRODUCED - Followers received the PONG.");
                    System.out.println("(This can happen if snapshot was taken, which calls loadState())");
                }
            }

        } finally {
            System.out.println("\nCleaning up...");
            cleanup();
        }
    }

    private static void startCluster() throws Exception {
        // All nodes use the SAME port base - ClusterConfig handles per-node offsets internally
        List<String> hostnames = List.of("localhost", "localhost", "localhost");

        for (int nodeId = 0; nodeId < NODE_COUNT; nodeId++) {
            String baseDir = "/tmp/aeron-bug-" + nodeId;
            IoUtil.delete(new File(baseDir), true);

            services[nodeId] = new TestService(nodeId);

            ClusterConfig config = ClusterConfig.create(
                    nodeId,
                    hostnames,
                    hostnames,
                    PORT_BASE,  // Same port base for all nodes
                    services[nodeId]
            );
            // Use the ingress channel with explicit endpoint - clients will connect to these
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

            // Give each node time to start before launching next
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
            IoUtil.delete(new File("/tmp/aeron-bug-" + i), true);
        }
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
                System.out.printf("    [Node %d] Received PING #%d (role=%s)%n",
                        nodeId, pingNumber, cluster.role());

                if (cluster.role() == Cluster.Role.LEADER) {
                    // Publish PONG via cluster.offer()
                    sendBuffer.putInt(0, MSG_PONG);
                    sendBuffer.putInt(4, pingNumber);
                    sendBuffer.putLong(8, System.nanoTime());

                    long result = cluster.offer(sendBuffer, 0, 16);
                    lastOfferResult.set(result);
                    pongsPublishedByLeader.incrementAndGet();

                    System.out.printf("    [Node %d] Published PONG #%d, cluster.offer() = %d%n",
                            nodeId, pingNumber, result);
                }

            } else if (templateId == MSG_PONG) {
                int pingNumber = buffer.getInt(offset + 4);

                // Count PONGs received by followers
                if (cluster.role() != Cluster.Role.LEADER) {
                    pongsReceivedByFollowers.incrementAndGet();
                    System.out.printf("    [Node %d] FOLLOWER received PONG #%d%n", nodeId, pingNumber);
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
            System.out.printf("    [Node %d] Role changed to %s%n", nodeId, newRole);
            if (newRole == Cluster.Role.LEADER) {
                currentLeader.set(nodeId);
            }
        }

        @Override
        public void onTerminate(Cluster cluster) {}

        @Override
        public void onNewLeadershipTermEvent(long leadershipTermId, long logPosition, long timestamp,
                                              long termBaseLogPosition, int leaderMemberId, int logSessionId,
                                              TimeUnit timeUnit, int appVersion) {
            System.out.printf("    [Node %d] New leadership term %d, leader=%d%n",
                    nodeId, leadershipTermId, leaderMemberId);
        }
    }

    // ==================== Test Client ====================

    static class TestClient implements EgressListener, AutoCloseable {
        private MediaDriver driver;
        private AeronCluster cluster;
        private final ExpandableArrayBuffer sendBuffer = new ExpandableArrayBuffer(64);
        private int excludeNode = -1;

        TestClient() {
            this(-1);
        }

        TestClient(int excludeNode) {
            this.excludeNode = excludeNode;
        }

        void connect() throws InterruptedException {
            // Wait for cluster to stabilize before connecting
            Thread.sleep(2000);

            String aeronDir = CommonContext.generateRandomDirName();

            // Build ingress endpoints - exclude killed node if specified
            StringBuilder ingressEndpoints = new StringBuilder();
            boolean first = true;
            for (int i = 0; i < NODE_COUNT; i++) {
                if (i == excludeNode) continue;  // Skip killed node
                if (!first) ingressEndpoints.append(",");
                first = false;
                int ingressPort = PORT_BASE + (i * 100) + 2;  // CLIENT_FACING_PORT_OFFSET = 2
                ingressEndpoints.append(i).append("=localhost:").append(ingressPort);
            }

            System.out.println("  Ingress endpoints: " + ingressEndpoints);

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

            System.out.println("  Client connected to leader: " + cluster.leaderMemberId());
        }

        int getLeaderId() {
            return cluster.leaderMemberId();
        }

        void sendPing(int pingNumber) {
            sendBuffer.putInt(0, MSG_PING);
            sendBuffer.putInt(4, pingNumber);

            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1000, 1_000_000);
            long result;
            while ((result = cluster.offer(sendBuffer, 0, 8)) < 0) {
                idle.idle();
            }
            System.out.printf("  Client sent PING #%d%n", pingNumber);
        }

        void poll() {
            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1000, 1_000_000);
            long deadline = System.currentTimeMillis() + 2000;
            while (System.currentTimeMillis() < deadline) {
                cluster.pollEgress();
                idle.idle();
            }
        }

        @Override
        public void onMessage(long clusterSessionId, long timestamp,
                              DirectBuffer buffer, int offset, int length, Header header) {
            // Client-side PONG handling if needed
        }

        @Override
        public void onSessionEvent(long correlationId, long clusterSessionId,
                                    long leadershipTermId, int leaderMemberId,
                                    EventCode code, String detail) {
        }

        @Override
        public void onNewLeader(long clusterSessionId, long leadershipTermId,
                                int leaderMemberId, String ingressEndpoints) {
            System.out.printf("  Client: New leader notification - Node %d%n", leaderMemberId);
        }

        @Override
        public void close() {
            CloseHelper.quietClose(cluster);
            CloseHelper.quietClose(driver);
        }
    }
}
