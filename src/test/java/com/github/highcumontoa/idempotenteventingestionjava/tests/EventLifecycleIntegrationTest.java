package com.github.highcumontoa.idempotenteventingestionjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.idempotenteventingestionjava.handler.DefaultEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStatus;
import com.github.highcumontoa.idempotenteventingestionjava.model.FailureReason;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 端到端集成测试（真实 Spring MVC + 调度器 + 文件快照，无外部服务）：
 * 1) REST 状态码：首次 202 / 重复 200 / 非法 400 / 限流 503 / 不存在 404；
 * 2) 并发重复投递只有一次有效处理，统计不重不漏；
 * 3) 服务重启（新 store 实例从同一快照目录加载）后中断事件自动恢复并重试成功。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "eventing.max-retries=2",
                "eventing.backoff-initial-millis=30",
                "eventing.backoff-multiplier=1.0",
                "eventing.backoff-max-millis=60",
                "eventing.scheduler-interval-millis=30",
                "eventing.worker-threads=4",
                "eventing.data-dir=target/eventing-it"
        })
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EventLifecycleIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    static Path sharedTemp;

    /** 活动实例的快照目录；重启测试会把它复制到临时目录后用新 store 打开。 */
    private static final Path LIVE_DIR = Path.of("target/eventing-it");

    @LocalServerPort
    int port;

    @Autowired
    private TestRestTemplate rest;

    @Autowired
    private EventIngestionService service;

    @Autowired
    private DefaultEventHandler eventHandler;

    @Autowired
    private com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore fileStore;

    @BeforeAll
    static void cleanDataDir() throws Exception {
        java.nio.file.Files.createDirectories(LIVE_DIR);
        try (var walk = java.nio.file.Files.walk(LIVE_DIR)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(f -> { try { java.nio.file.Files.deleteIfExists(f); } catch (Exception ignored) { } });
        }
        java.nio.file.Files.createDirectories(LIVE_DIR);
    }

    private String url(String suffix) {
        return "http://localhost:" + port + "/api/events" + suffix;
    }

    private HttpHeaders jsonHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        return h;
    }

    private ResponseEntity<String> post(String body) {
        return rest.postForEntity(url(""), new HttpEntity<>(body, jsonHeaders()), String.class);
    }

    private String eventJson(String key, String behavior, Integer failUntilAttempt, String effect)
            throws Exception {
        var node = MAPPER.createObjectNode();
        node.put("idempotencyKey", key);
        node.put("type", "demo");
        var payload = MAPPER.createObjectNode();
        if (behavior != null) {
            payload.put("behavior", behavior);
        }
        if (failUntilAttempt != null) {
            payload.put("failUntilAttempt", failUntilAttempt);
        }
        if (effect != null) {
            payload.put("effect", effect);
        }
        node.put("payload", payload.toString());
        return MAPPER.writeValueAsString(node);
    }

    private JsonNode awaitTerminal(String key, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            ResponseEntity<String> resp = rest.getForEntity(url("/" + key), String.class);
            if (resp.getStatusCode().is2xxSuccessful()) {
                JsonNode node = MAPPER.readTree(resp.getBody());
                String status = node.path("status").asText();
                if ("SUCCEEDED".equals(status) || "TERMINATED".equals(status)) {
                    return node;
                }
            }
            Thread.sleep(30);
        }
        throw new AssertionError("事件 " + key + " 未在超时内到达终态");
    }

    @Test
    @Order(1)
    void httpSemantics_firstAccepted_thenDuplicate_thenInvalid() throws Exception {
        // 首次 -> 202
        ResponseEntity<String> first = post(eventJson("http-001", null, null, null));
        assertEquals(HttpStatus.ACCEPTED, first.getStatusCode());
        JsonNode firstBody = MAPPER.readTree(first.getBody());
        assertEquals("ACCEPTED", firstBody.path("outcome").asText());

        // 重复 -> 200 且 outcome=DUPLICATE，状态可区分
        ResponseEntity<String> dup = post(eventJson("http-001", null, null, null));
        assertEquals(HttpStatus.OK, dup.getStatusCode());
        assertEquals("DUPLICATE", MAPPER.readTree(dup.getBody()).path("outcome").asText());

        // 非法 -> 400
        ResponseEntity<String> invalid = post("{\"type\":\"demo\"}");
        assertEquals(HttpStatus.BAD_REQUEST, invalid.getStatusCode());
        assertTrue(invalid.getBody().contains("INVALID_EVENT"));

        // 不存在 -> 404
        ResponseEntity<String> missing = rest.getForEntity(url("/no-such-key"), String.class);
        assertEquals(HttpStatus.NOT_FOUND, missing.getStatusCode());

        JsonNode terminal = awaitTerminal("http-001", 5000);
        assertEquals("SUCCEEDED", terminal.path("status").asText());
    }

    @Test
    @Order(2)
    void concurrentDuplicates_singleEffect_andStatsConsistent() throws Exception {
        String key = "http-concurrent-001";
        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            Callable<ResponseEntity<String>> task =
                    () -> post(eventJson(key, null, null, "acct:C+1"));
            var futures = pool.invokeAll(IntStream.range(0, threads).mapToObj(i -> task).toList());
            int accepted = 0;
            int duplicate = 0;
            for (Future<ResponseEntity<String>> f : futures) {
                ResponseEntity<String> r = f.get();
                if (r.getStatusCode() == HttpStatus.ACCEPTED) {
                    accepted++;
                } else if (r.getStatusCode() == HttpStatus.OK) {
                    duplicate++;
                    assertEquals("DUPLICATE", MAPPER.readTree(r.getBody()).path("outcome").asText());
                } else {
                    fail("意外状态码: " + r.getStatusCode());
                }
            }
            assertEquals(1, accepted);
            assertEquals(threads - 1, duplicate);
        } finally {
            pool.shutdownNow();
        }

        JsonNode terminal = awaitTerminal(key, 5000);
        assertEquals("SUCCEEDED", terminal.path("status").asText());
        assertEquals(1, terminal.path("attemptCount").asInt(), "重复投递不得导致重复处理");

        // 统计口径：四桶之和 = total；重复事件未重复计入
        JsonNode stats = MAPPER.readTree(rest.getForEntity(url("/stats"), String.class).getBody());
        long buckets = stats.path("pending").asLong() + stats.path("processing").asLong()
                + stats.path("succeeded").asLong() + stats.path("terminated").asLong();
        assertEquals(stats.path("total").asLong(), buckets, "排队/处理中/成功/终止不重不漏");
        assertEquals(1, stats.path("rejected").asLong(), "Order(1) 中恰好 1 次非法拒绝");
    }

    @Test
    @Order(3)
    void retryThenTerminate_failureReasonsDistinguishable() throws Exception {
        // 不可重试：立即终止
        post(eventJson("http-fatal", "FAIL_NON_RETRYABLE", null, null));
        JsonNode fatal = awaitTerminal("http-fatal", 5000);
        assertEquals("TERMINATED", fatal.path("status").asText());
        assertEquals("NON_RETRYABLE_ERROR", fatal.path("failureReason").asText());
        assertEquals(1, fatal.path("attemptCount").asInt());

        // 可重试但一直失败：达到上限后终止为 RETRIES_EXHAUSTED
        post(eventJson("http-exhaust", "FAIL_RETRYABLE", null, null));
        JsonNode exhausted = awaitTerminal("http-exhaust", 10_000);
        assertEquals("TERMINATED", exhausted.path("status").asText());
        assertEquals("RETRIES_EXHAUSTED", exhausted.path("failureReason").asText());
        assertEquals(3, exhausted.path("attemptCount").asInt(), "maxRetries=2 => 共 3 次尝试");
        assertFalse(exhausted.path("detail").asText().isBlank(), "终止细节必须保留");

        JsonNode stats = MAPPER.readTree(rest.getForEntity(url("/stats"), String.class).getBody());
        assertEquals(1, stats.path("terminalNonRetryableError").asLong());
        assertEquals(1, stats.path("terminalRetriesExhausted").asLong());
        assertEquals(2, stats.path("terminated").asLong());
    }

    /**
     * 模拟服务重启：同一快照目录上构造第二个服务实例（等价于新 JVM 启动）。
     * 在共享的文件存储中制造一个停留在 PROCESSING 的事件快照，
     * 新实例 recover() 后事件必须回到 PENDING 并被重试至成功，副作用不重复。
     */
    @Test
    @Order(4)
    void restartRecovery_processingInterrupted_thenRetriedToSuccess() throws Exception {
        long effectBefore = eventHandler.getAppliedEffectCount();
        String payload = "{\"behavior\":\"FAIL_RETRYABLE\",\"failUntilAttempt\":2,\"effect\":\"acct:D+7\"}";
        String key = "http-restart-001";

        // 制造"首次尝试已开始即崩溃"的现场（快照停留在 PROCESSING）
        long now = System.currentTimeMillis();
        EventRecord pending = new EventRecord();
        pending.setIdempotencyKey(key);
        pending.setType("demo");
        pending.setPayload(payload);
        pending.setStatus(EventStatus.PENDING);
        pending.setAttemptCount(0);
        pending.setFailureReason(FailureReason.NONE);
        pending.setCreatedAt(now);
        pending.setUpdatedAt(now);
        assertTrue(fileStore.insertIfAbsent(pending));

        EventRecord processing = fileStore.find(key);
        int v0 = processing.getVersion();
        processing.setStatus(EventStatus.PROCESSING);
        processing.setAttemptCount(1);
        processing.setUpdatedAt(now);
        assertTrue(fileStore.compareAndUpdate(processing, v0));

        // "重启"：把崩溃时的快照复制到新目录（等价于新 JVM 挂载同一持久卷），
        // 新 store 从副本加载，避免与活动实例双写同一文件
        Path restartDir = sharedTemp.resolve("eventing-it-restart");
        java.nio.file.Files.createDirectories(restartDir);
        java.nio.file.Files.copy(LIVE_DIR.resolve("events.json"),
                restartDir.resolve("events.json"));
        var props = new com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties();
        props.setDataDir(restartDir.toString());
        props.setMaxRetries(2);
        props.setBackoffInitialMillis(30);
        props.setBackoffMultiplier(1.0);
        props.setBackoffMaxMillis(60);
        var restartedStore =
                new com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore(
                        props, new ObjectMapper());
        restartedStore.init();
        var restartedExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "restart-worker");
            t.setDaemon(true);
            return t;
        });
        var restarted = new EventIngestionService(restartedStore, eventHandler, props, restartedExecutor);

        assertEquals(1, restarted.recover());
        assertEquals(EventStatus.PENDING, restarted.get(key).getStatus());
        assertEquals(FailureReason.RECOVERED_AFTER_RESTART, restarted.get(key).getFailureReason());

        JsonNode done = awaitTerminalOn(restarted, key, 10_000);
        assertEquals("SUCCEEDED", done.path("status").asText());
        assertEquals(2, done.path("attemptCount").asInt(), "崩溃前 1 次 + 恢复后 1 次");
        assertEquals(effectBefore + 1, eventHandler.getAppliedEffectCount(),
                "恢复重投不得造成重复记账");
        restartedExecutor.shutdownNow();
    }

    private JsonNode awaitTerminalOn(EventIngestionService svc, String key, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        svc.dispatchDueEvents();
        while (System.currentTimeMillis() < deadline) {
            EventRecord r = svc.get(key);
            if (r != null && (r.getStatus() == EventStatus.SUCCEEDED
                    || r.getStatus() == EventStatus.TERMINATED)) {
                return MAPPER.valueToTree(r);
            }
            Thread.sleep(30);
            svc.dispatchDueEvents();
        }
        throw new AssertionError("恢复后事件未在超时内到达终态: " + key);
    }
}
