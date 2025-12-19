/*
 * Test client that sends PING messages to the cluster and tracks PONG responses.
 * Used to demonstrate the service message drop bug.
 */
package io.aeron.bug;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client for testing the service message drop bug.
 *
 * Usage:
 * 1. Start 3 cluster nodes (nodes 0, 1, 2)
 * 2. Run client: sends PINGs, expects PONGs back via egress
 * 3. Kill the leader node
 * 4. Send more PINGs
 * 5. Observe: cluster.offer() returns success but PONGs are never received
 */
public class TestClient implements EgressListener {

    private static final int MSG_PING = 1;
    private static final int MSG_PONG = 2;

    private final AtomicInteger pingsSent = new AtomicInteger(0);
    private final AtomicInteger pongsReceived = new AtomicInteger(0);

    private final ExpandableArrayBuffer sendBuffer = new ExpandableArrayBuffer(64);

    @Override
    public void onMessage(long clusterSessionId, long timestamp,
                          DirectBuffer buffer, int offset, int length,
                          Header header) {
        int templateId = buffer.getInt(offset);

        if (templateId == MSG_PONG) {
            int pingNumber = buffer.getInt(offset + 4);
            pongsReceived.incrementAndGet();
            System.out.printf("[Client] Received PONG #%d%n", pingNumber);
        } else {
            System.out.printf("[Client] Received unknown message type: %d%n", templateId);
        }
    }

    @Override
    public void onSessionEvent(long correlationId, long clusterSessionId,
                                long leadershipTermId, int leaderMemberId,
                                EventCode code, String detail) {
        System.out.printf("[Client] Session event: code=%s, leader=%d, detail=%s%n",
                code, leaderMemberId, detail);
    }

    @Override
    public void onNewLeader(long clusterSessionId, long leadershipTermId,
                            int leaderMemberId, String ingressEndpoints) {
        System.out.printf("[Client] *** NEW LEADER: node %d (term %d) ***%n",
                leaderMemberId, leadershipTermId);
    }

    public void sendPing(AeronCluster cluster, int pingNumber) {
        sendBuffer.putInt(0, MSG_PING);
        sendBuffer.putInt(4, pingNumber);

        IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1000, 1_000_000);
        long result;
        while ((result = cluster.offer(sendBuffer, 0, 8)) < 0) {
            idle.idle();
        }

        pingsSent.incrementAndGet();
        System.out.printf("[Client] Sent PING #%d, result=%d%n", pingNumber, result);
    }

    public void printStats() {
        System.out.printf("[Client] Stats: sent=%d, received=%d%n",
                pingsSent.get(), pongsReceived.get());

        if (pingsSent.get() > pongsReceived.get()) {
            System.out.printf("[Client] *** MISSING %d PONGs! ***%n",
                    pingsSent.get() - pongsReceived.get());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(70));
        System.out.println("Aeron Cluster Service Message Drop Bug Reproducer - Client");
        System.out.println("=".repeat(70));
        System.out.println();
        System.out.println("This client will:");
        System.out.println("1. Connect to the 3-node cluster");
        System.out.println("2. Send PING messages (the service responds with PONG via cluster.offer())");
        System.out.println("3. After failover, observe that PONGs are silently dropped");
        System.out.println();
        System.out.println("Instructions:");
        System.out.println("  - Start nodes 0, 1, 2 first");
        System.out.println("  - Run this client");
        System.out.println("  - Send a few PINGs (type a number and press Enter)");
        System.out.println("  - Kill the leader node (Ctrl+C on that terminal)");
        System.out.println("  - Send more PINGs");
        System.out.println("  - Observe: cluster.offer() succeeds but PONGs are never received!");
        System.out.println();
        System.out.println("Commands: <number> = send PING, 'q' = quit, 's' = stats");
        System.out.println("=".repeat(70));

        TestClient client = new TestClient();

        // Build ingress endpoints for all 3 nodes
        // Format: 0=hostname:port,1=hostname:port,2=hostname:port
        // ClusterConfig uses: portBase + (nodeId * 100) + CLIENT_FACING_PORT_OFFSET (2)
        StringBuilder ingressEndpoints = new StringBuilder();
        int portBase = 9000;
        for (int i = 0; i < 3; i++) {
            if (i > 0) ingressEndpoints.append(",");
            int ingressPort = portBase + (i * 100) + 2;  // CLIENT_FACING_PORT_OFFSET = 2
            ingressEndpoints.append(i).append("=localhost:").append(ingressPort);
        }

        System.out.println("Ingress endpoints: " + ingressEndpoints);

        String aeronDir = CommonContext.generateRandomDirName();

        try (MediaDriver driver = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                        .aeronDirectoryName(aeronDir)
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true));
             AeronCluster cluster = AeronCluster.connect(
                     new AeronCluster.Context()
                             .aeronDirectoryName(aeronDir)
                             .egressListener(client)
                             .ingressChannel("aeron:udp")
                             .egressChannel("aeron:udp?endpoint=localhost:0")
                             .ingressEndpoints(ingressEndpoints.toString()))) {

            System.out.println("[Client] Connected to cluster");

            IdleStrategy idle = new BackoffIdleStrategy(100, 100, 1000, 1_000_000);
            java.util.Scanner scanner = new java.util.Scanner(System.in);
            int pingNumber = 1;

            while (true) {
                // Poll for responses
                cluster.pollEgress();

                System.out.print("Enter ping number (or 'q' to quit, 's' for stats): ");
                String input = scanner.nextLine().trim();

                if (input.equalsIgnoreCase("q")) {
                    break;
                } else if (input.equalsIgnoreCase("s")) {
                    client.printStats();
                } else {
                    try {
                        int num = Integer.parseInt(input);
                        client.sendPing(cluster, num);

                        // Poll for response
                        System.out.println("Waiting for PONG...");
                        long deadline = System.currentTimeMillis() + 5000;
                        int beforeCount = client.pongsReceived.get();

                        while (System.currentTimeMillis() < deadline) {
                            if (cluster.pollEgress() > 0) {
                                if (client.pongsReceived.get() > beforeCount) {
                                    break;
                                }
                            }
                            idle.idle();
                        }

                        if (client.pongsReceived.get() == beforeCount) {
                            System.out.println("*** TIMEOUT: No PONG received! ***");
                            System.out.println("*** This indicates the bug - cluster.offer() succeeded but message was dropped ***");
                        }

                    } catch (NumberFormatException e) {
                        System.out.println("Invalid input. Enter a number, 'q', or 's'");
                    }
                }
            }

            client.printStats();
        }
    }
}
