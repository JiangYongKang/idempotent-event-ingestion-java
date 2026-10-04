package com.github.highcumontoa.idempotenteventingestionjava.tests;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.github.highcumontoa.idempotenteventingestionjava.handler.DemoLedgerEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventProcessingService;
import java.nio.file.Path;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
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
 * 全栈 HTTP 语义测试：首次接受 202、重复 200、非法 400、超限 503、统计口径一致；
 * 终止事件重跑 202 / 404 / 409。
 * 使用独立临时数据文件并关闭后台 worker，保证测试确定且不污染本地 data/。
 * （用例按序执行：重跑用例复用接入用例释放后的容量。）
 */
@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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

    @Autowired
    private EventProcessingService processingService;

    @Autowired
    private EventHandler eventHandler;

    @Test
    @Order(1)
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

    @Test
    @Order(2)
    void httpSemanticsForRerun() throws Exception {
        // 释放上一个 用例1 占用的在途容量（容量=1）：把遗留 QUEUED 事件处理成功
        processingService.processDueBatch();

        String key = "api-rerun-" + System.nanoTime();

        // 重跑不存在的键 -> 404 NOT_FOUND
        mockMvc.perform(post("/api/events/missing-" + System.nanoTime() + "/rerun"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.rejectionReason").value("NOT_FOUND"));
        log.info("[测试] 幂等键=<不存在> 判定依据=HTTP 404 NOT_FOUND 重跑拒绝");

        // 接入一条 FIXABLE 事件并跑满 3 次尝试直至终止
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"idempotencyKey\":\"" + key + "\",\"payload\":\"FIXABLE:下游临时挂了\"}"))
                .andExpect(status().isAccepted());
        for (int i = 0; i < 3; i++) {
            Thread.sleep(50);
            processingService.processDueBatch();
        }
        mockMvc.perform(get("/api/events/" + key))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("TERMINATED"))
                .andExpect(jsonPath("$.terminationReason").value("RETRY_EXHAUSTED"))
                .andExpect(jsonPath("$.rerunCount").value(0));
        log.info("[测试] 幂等键={} 轮次=0 重试次数=3 判定依据=达到上限RETRY_EXHAUSTED终止", key);

        // 故障修复后发起重跑 -> 202，重新入队，轮次推进
        ((DemoLedgerEventHandler) eventHandler).markFixed(key);
        mockMvc.perform(post("/api/events/" + key + "/rerun"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED"))
                .andExpect(jsonPath("$.record.status").value("QUEUED"))
                .andExpect(jsonPath("$.record.rerunCount").value(1))
                .andExpect(jsonPath("$.record.rerunHistory[0].terminationReason")
                        .value("RETRY_EXHAUSTED"));
        log.info("[测试] 幂等键={} 轮次=1 判定依据=HTTP 202 重跑入队,上一轮终止原因已归档", key);

        // 已重新入队（非终止态）再次重跑 -> 409 NOT_TERMINATED，与轮数到顶可区分
        mockMvc.perform(post("/api/events/" + key + "/rerun"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.rejectionReason").value("NOT_TERMINATED"));
        log.info("[测试] 幂等键={} 判定依据=HTTP 409 NOT_TERMINATED 非终止态拒绝重跑", key);

        // 重跑轮处理成功：拿到结果凭证，历史保留
        processingService.processDueBatch();
        mockMvc.perform(get("/api/events/" + key))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"))
                .andExpect(jsonPath("$.result").isNotEmpty())
                .andExpect(jsonPath("$.rerunCount").value(1))
                .andExpect(jsonPath("$.rerunHistory[0].round").value(0));
        log.info("[测试] 幂等键={} 轮次=1 判定依据=重跑成功SUCCEEDED,结果凭证与历史归档可查", key);

        // 统计口径依然平：重跑只是状态迁移，不增删记录
        mockMvc.perform(get("/api/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consistent").value(true));
    }
}
