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
        registry.add("eventing.retry-base-delay", () -> "10ms");
    }

    private static final Logger log = LoggerFactory.getLogger(EventApiIntegrationTest.class);

    @Autowired
    private MockMvc mockMvc;

    @Test
    void httpSemanticsForAcceptDuplicateRejectThrottleAndStats() throws Exception {
        String key = "api-key-" + System.nanoTime();

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

        // 统计：1 条 QUEUED，total=1，口径一致
        mockMvc.perform(get("/api/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queued").value(1))
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.consistent").value(true));
        log.info("[测试] 幂等键={} 判定依据=统计 queued=1 total=1 consistent=true,重复未计入", key);
    }
}
