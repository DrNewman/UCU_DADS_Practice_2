package com.ucu.raft;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ucu.raft.model.LogEntry;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LocalAcceptanceIT {

    private static final String JAR_PATH = "target/raft-node-0.0.1-SNAPSHOT.jar";

    private static final Logger log = LoggerFactory.getLogger(LocalAcceptanceIT.class);
    private static final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Map<Integer, Integer> HTTP_PORTS = Map.of(1, 18020, 2, 18021, 3, 18022);
    private static final Map<String, Process> processes = new HashMap<>();

    public record StateResponse(String nodeId, String state, long currentTerm, long commitIndex, long lastApplied,  List<LogEntry> log) {}

    @BeforeAll
    static void startProcesses() throws Exception {
        // Запускаємо ноди через локальний java -jar
        log.info("Using JAR file: {}", JAR_PATH);

        startNode(1);
        startNode(2);
        startNode(3);
    }

    private static void startNode(int id) throws IOException {
        String peers = String.format("http://localhost:%d,http://localhost:%d",
                id == 1 ? HTTP_PORTS.get(2) : HTTP_PORTS.get(1),
                id == 3 ? HTTP_PORTS.get(2) : HTTP_PORTS.get(3));

        ProcessBuilder pb = new ProcessBuilder(
                "java", "-jar", JAR_PATH,
                "--server.port=" + HTTP_PORTS.get(id),
                "--raft.node-id=node" + id,
                "--raft.peers=" + peers
        );
        pb.inheritIO();
        processes.put("node" + id, pb.start());
    }

    @AfterAll
    static void stopProcesses() {
        processes.values().forEach(Process::destroyForcibly);
    }

    @Test
    @Order(1)
    @DisplayName("Крок 1-3: Вибори, реплікація та синхронізація 3-ї ноди")
    void testElectionAndReplication() throws Exception {
        StateResponse leader = awaitSingleLeader(List.of("node1", "node2", "node3"), 0);
        assertNotNull(leader);

        int nodeId = Integer.parseInt(leader.nodeId().replace("node", ""));
        postCommand(HTTP_PORTS.get(nodeId), "msg1");
        postCommand(HTTP_PORTS.get(nodeId), "msg2");

        awaitConverged(List.of(1, 2, 3), List.of("msg1", "msg2"), 2);
    }

    @Test
    @Order(2)
    @DisplayName("Крок 4-7: Ізоляція лідера та реплікація після відновлення")
    void testPartitionAndOverwriting() throws Exception {
        StateResponse oldLeaderState = awaitSingleLeader(List.of("node1", "node2", "node3"), 0);
        String oldLeaderId = oldLeaderState.nodeId();

        // 1. Зупиняємо старого лідера (імітація network partition)
        processes.get(oldLeaderId).destroyForcibly();

        List<String> majorityIds = new ArrayList<>(List.of("node1", "node2", "node3"));
        majorityIds.remove(oldLeaderId);

        // 2. Новий лідер обирається серед більшості
        StateResponse newLeaderState = awaitSingleLeader(majorityIds, oldLeaderState.currentTerm());
        int newLeaderPort = HTTP_PORTS.get(Integer.parseInt(newLeaderState.nodeId().replace("node", "")));

        // 3. Відправляємо msg3 і msg4 новому лідеру
        postCommand(newLeaderPort, "msg3");
        postCommand(newLeaderPort, "msg4");

        // 4. Відновлюємо старого лідера назад
        startNode(Integer.parseInt(oldLeaderId.replace("node", "")));

        // 5. Перевіряємо, що лог синхронізувався
        awaitConverged(List.of(1, 2, 3), List.of("msg1", "msg2", "msg3", "msg4"), 4);
    }

    private static StateResponse getState(int port) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/api/v1/state"))
                    .GET()
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            return objectMapper.readValue(resp.body(), StateResponse.class);
        } catch (Exception e) {
            // Пропускаємо поки виклик не пройде успішно
            return new StateResponse("unknown", "OFFLINE", -1, -1, -1, List.of());
        }
    }

    private static void postCommand(int port, String command) throws Exception {

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/api/v1/command"))
                .header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString(command))
                .build();

        httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static StateResponse awaitSingleLeader(List<String> nodes, long minTerm) {
        final StateResponse[] result = new StateResponse[1];
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).until(() -> {
            int count = 0;
            for (String node : nodes) {
                int port = HTTP_PORTS.get(Integer.parseInt(node.replace("node", "")));
                StateResponse st = getState(port);
                if ("LEADER".equalsIgnoreCase(st.state()) && st.currentTerm() > minTerm) {
                    count++;
                    result[0] = st;
                }
            }
            return count == 1;
        });
        return result[0];
    }

    private static void awaitConverged(List<Integer> nodeIds, List<String> expectedCommands, int expectedCommitIndex) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            for (int id : nodeIds) {
                int port = HTTP_PORTS.get(id);

                StateResponse st = getState(port);

                // Перевірка commitIndex
                assertEquals(expectedCommitIndex, st.commitIndex(), "Commit index mismatch on node" + id);

                // Перевірка логу
                for (String key : expectedCommands) {
                    List<String> actualCommands = st.log().stream().map(LogEntry::getCommand).toList();
                    assertTrue(actualCommands.contains(key),
                            String.format("Key '%s' is missing in state machine on node%d", key, id));
                }
            }
        });
    }
}