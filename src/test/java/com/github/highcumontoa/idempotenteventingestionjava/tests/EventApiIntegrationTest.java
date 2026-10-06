package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

/**
 * 全栈 HTTP 语义测试：首次接受 202、重复 200、非法 400、超限 503、统计口径一致。
 * 使用独立临时数据文件并关闭后台 worker，保证测试确定且不污染本地 data/。
 */
@SpringBootTest
@AutoConfigureMockMvc
class EventApiIntegrationTest {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("eventing.data-file", () -> tempDir.resolve("events-it.dat").toString());
        registry.add("eventing.worker-enabled", () -> "false");
        registry.add("eventing.max-queue-capacity", () -> "1");
        registry.add("eventing.max-attempts", () -> "3");
        registry.add("eventing.max-reruns", () -> "1");
        registry.add("eventing.retry-base-delay", () -> "10ms");
    }

    private static final Logger log = LoggerFactory.getLogger(EventApiIntegrationTest.class);

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private com.github.highcumontoa.idempotenteventingestionjava.service.EventProcessingService processor;

    @Test
    void httpSemanticsForAcceptDuplicateRejectThrottleAndStats() throws Exception {
        String key = "api-key-" + System.nanoTime();

        // 统计基线（同一 Spring 上下文内多个测试方法共享数据文件，按增量断言）
        long[] baseline = statsSnapshot();

        // 首次接受 202
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + key + "\",\"payload\":\"NORMAL\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED"))
                .andExpect(jsonPath("$.record.status").value("QUEUED"));
        log.info("[测试] 幂等键={} 判定依据=HTTP 202 ACCEPTED", key);

        // 重复 200 DUPLICATE，且不会因重复而改变状态/计数
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + key + "\",\"payload\":\"NORMAL\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("DUPLICATE"));
        log.info("[测试] 幂等键={} 判定依据=HTTP 200 DUPLICATE 不重复处理", key);

        // 非法 400
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"\",\"payload\":\"x\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.rejectionReason").value("INVALID_EVENT"));
        log.info("[测试] 幂等键=<空> 判定依据=HTTP 400 INVALID_EVENT 明确拒绝");

        // 容量 1 已被上面的 QUEUED 占满 -> 503 限流
        String other = "api-other-" + System.nanoTime();
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + other + "\",\"payload\":\"NORMAL\"}"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.rejectionReason").value("QUEUE_FULL"));
        log.info("[测试] 幂等键={} 判定依据=HTTP 503 QUEUE_FULL 限流不阻塞", other);

        // 查询不存在 -> 404
        mockMvc.perform(get("/api/events/missing-" + System.nanoTime()))
                .andExpect(status().isNotFound());

        // 统计：新增 1 条 QUEUED，口径一致（按基线增量断言）
        mockMvc.perform(get("/api/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queued").value(baseline[0] + 1))
                .andExpect(jsonPath("$.total").value(baseline[1] + 1))
                .andExpect(jsonPath("$.consistent").value(true));
        log.info("[测试] 幂等键={} 判定依据=统计 queued=基线+1 total=基线+1 consistent=true,重复未计入", key);
    }

    /** 读取当前 [queued, total] 作为增量断言基线。 */
    private long[] statsSnapshot() throws Exception {
        String json = mockMvc.perform(get("/api/stats"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        com.fasterxml.jackson.databind.JsonNode node =
                new com.fasterxml.jackson.databind.ObjectMapper().readTree(json);
        return new long[]{node.get("queued").asLong(), node.get("total").asLong()};
    }

    @Test
    void rerunEndpointSemantics() throws Exception {
        String key = "api-rerun-" + System.nanoTime();

        // 同一上下文共享数据文件：先排空其他用例遗留的在途事件，避免占用容量（上限=1）
        processor.processDueBatch();

        // 不存在的键 -> 404 NOT_FOUND
        mockMvc.perform(post("/api/events/missing-" + System.nanoTime() + "/rerun"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.rejectionReason").value("NOT_FOUND"));
        log.info("[测试] 幂等键=<不存在> 判定依据=HTTP 404 NOT_FOUND");

        // 制造终止事件：FATAL 一次处理即 NON_RETRYABLE 终止（worker 关闭，手工驱动）
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + key + "\",\"payload\":\"FATAL:脏数据\"}"))
                .andExpect(status().isAccepted());
        processor.processDueBatch();
        mockMvc.perform(get("/api/events/" + key))
                .andExpect(jsonPath("$.status").value("TERMINATED"))
                .andExpect(jsonPath("$.terminationReason").value("NON_RETRYABLE"));
        log.info("[测试] 幂等键={} 处理次数=1 判定依据=不可重试失败终止,具备重跑前提", key);

        // 发起重跑 -> 202 ACCEPTED，重新入队，历史保留
        mockMvc.perform(post("/api/events/" + key + "/rerun"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED"))
                .andExpect(jsonPath("$.record.status").value("QUEUED"))
                .andExpect(jsonPath("$.record.rerunCount").value(1))
                .andExpect(jsonPath("$.record.attempts").value(1))
                .andExpect(jsonPath("$.record.lastTerminationReason").value("NON_RETRYABLE"))
                // 重跑已入队、新轮尚未跑完：历史里仍完整保留首轮（round=0）的终止归档
                .andExpect(jsonPath("$.record.roundHistory.length()").value(1))
                .andExpect(jsonPath("$.record.roundHistory[0].round").value(0))
                .andExpect(jsonPath("$.record.roundHistory[0].attempts").value(1))
                .andExpect(jsonPath("$.record.roundHistory[0].outcome").value("TERMINATED"))
                .andExpect(jsonPath("$.record.roundHistory[0].terminationReason").value("NON_RETRYABLE"));
        log.info("[测试] 幂等键={} 重跑轮数=1 判定依据=HTTP 202 重新入队,首轮终止归档在roundHistory中独立保留", key);

        // 排队中再发重跑 -> 409 NOT_TERMINATED（与轮数到顶可区分）
        mockMvc.perform(post("/api/events/" + key + "/rerun"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.rejectionReason").value("NOT_TERMINATED"));
        log.info("[测试] 幂等键={} 判定依据=HTTP 409 NOT_TERMINATED 当前状态不允许重跑", key);

        // 重跑轮再次终止（max-reruns=1 已用满）-> 409 RERUN_LIMIT_REACHED
        processor.processDueBatch();
        mockMvc.perform(get("/api/events/" + key))
                .andExpect(jsonPath("$.status").value("TERMINATED"))
                .andExpect(jsonPath("$.rerunCount").value(1))
                .andExpect(jsonPath("$.attempts").value(2))
                // 两轮终止各占一条历史，互不覆盖：round=0 与 round=1 都能单独看清
                .andExpect(jsonPath("$.roundHistory.length()").value(2))
                .andExpect(jsonPath("$.roundHistory[0].round").value(0))
                .andExpect(jsonPath("$.roundHistory[0].attempts").value(1))
                .andExpect(jsonPath("$.roundHistory[0].terminationReason").value("NON_RETRYABLE"))
                .andExpect(jsonPath("$.roundHistory[1].round").value(1))
                .andExpect(jsonPath("$.roundHistory[1].attempts").value(1))
                .andExpect(jsonPath("$.roundHistory[1].terminationReason").value("NON_RETRYABLE"));
        log.info("[测试] 幂等键={} 判定依据=两轮终止各一条roundHistory(round0/round1独立),HTTP逐轮可查", key);
        mockMvc.perform(post("/api/events/" + key + "/rerun"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.rejectionReason").value("RERUN_LIMIT_REACHED"));
        log.info("[测试] 幂等键={} 重跑轮数=1/1 判定依据=HTTP 409 RERUN_LIMIT_REACHED 轮数到顶明确拒绝", key);

        // 统计口径不受重跑拒绝影响
        mockMvc.perform(get("/api/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consistent").value(true));
    }
}
