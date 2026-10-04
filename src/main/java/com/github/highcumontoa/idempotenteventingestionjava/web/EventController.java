package com.github.highcumontoa.idempotenteventingestionjava.web;

import com.github.highcumontoa.idempotenteventingestionjava.dto.EventRequest;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventStats;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResult;
import com.github.highcumontoa.idempotenteventingestionjava.model.RejectionReason;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunRejectionReason;
import com.github.highcumontoa.idempotenteventingestionjava.model.RerunResult;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventQueryService;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventRerunService;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST 接口：
 * <ul>
 *   <li>POST /api/events          —— 接入：202 首次接受 / 200 重复 / 400 非法 / 503 限流；</li>
 *   <li>POST /api/events/{key}/rerun —— 终止事件受控重跑：202 重新入队 /
 *       404 不存在 / 409 状态不允许或轮数到顶 / 503 容量不足；</li>
 *   <li>GET  /api/events/{key}    —— 查询单个事件状态（200 / 404）；</li>
 *   <li>GET  /api/stats           —— 四类状态统计，互斥且合计为 total。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class EventController {

    private final EventIngestionService ingestionService;
    private final EventRerunService rerunService;
    private final EventQueryService queryService;

    public EventController(EventIngestionService ingestionService, EventRerunService rerunService,
                           EventQueryService queryService) {
        this.ingestionService = ingestionService;
        this.rerunService = rerunService;
        this.queryService = queryService;
    }

    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> ingest(@RequestBody EventRequest request) {
        String key = request == null ? null : request.getIdempotencyKey();
        String payload = request == null ? null : request.getPayload();
        IngestionResult result = ingestionService.ingest(key, payload);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("outcome", result.outcome());
        body.put("message", result.message());

        return switch (result.outcome()) {
            case ACCEPTED -> {
                body.put("record", view(result.record()));
                yield ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
            }
            case DUPLICATE -> {
                body.put("record", view(result.record()));
                yield ResponseEntity.ok(body);
            }
            case REJECTED -> {
                RejectionReason reason = result.rejectionReason();
                body.put("rejectionReason", reason);
                HttpStatus status = reason == RejectionReason.QUEUE_FULL
                        ? HttpStatus.SERVICE_UNAVAILABLE
                        : HttpStatus.BAD_REQUEST;
                yield ResponseEntity.status(status).body(body);
            }
        };
    }

    @PostMapping("/events/{key}/rerun")
    public ResponseEntity<Map<String, Object>> rerun(@PathVariable String key) {
        RerunResult result = rerunService.rerun(key);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("outcome", result.outcome());
        body.put("message", result.message());

        return switch (result.outcome()) {
            case ACCEPTED -> {
                body.put("record", view(result.record()));
                yield ResponseEntity.status(HttpStatus.ACCEPTED).body(body);
            }
            case REJECTED -> {
                RerunRejectionReason reason = result.rejectionReason();
                body.put("rejectionReason", reason);
                HttpStatus status = switch (reason) {
                    case NOT_FOUND -> HttpStatus.NOT_FOUND;
                    case QUEUE_FULL -> HttpStatus.SERVICE_UNAVAILABLE;
                    // 状态不允许与轮数到顶都属“与当前资源状态冲突”，靠 rejectionReason 区分。
                    case NOT_TERMINATED, RERUN_LIMIT_REACHED -> HttpStatus.CONFLICT;
                };
                yield ResponseEntity.status(status).body(body);
            }
        };
    }

    @GetMapping("/events/{key}")
    public ResponseEntity<Map<String, Object>> getOne(@PathVariable String key) {
        EventRecord record = queryService.get(key);
        if (record == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(view(record));
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        EventStats s = queryService.stats();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("queued", s.queued());
        body.put("processing", s.processing());
        body.put("succeeded", s.succeeded());
        body.put("terminated", s.terminated());
        body.put("total", s.total());
        body.put("consistent", s.queued() + s.processing() + s.succeeded() + s.terminated() == s.total());
        return body;
    }

    private Map<String, Object> view(EventRecord r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("idempotencyKey", r.getIdempotencyKey());
        m.put("status", r.getStatus());
        m.put("attempts", r.getAttempts());
        m.put("roundAttempts", r.getRoundAttempts());
        m.put("rerunCount", r.getRerunCount());
        m.put("lastError", r.getLastError());
        m.put("lastErrorRetryable", r.isLastErrorRetryable());
        m.put("terminationReason", r.getTerminationReason());
        m.put("lastTerminationReason", r.getLastTerminationReason());
        m.put("lastTerminatedAt", r.getLastTerminatedAt());
        m.put("result", r.getResult());
        m.put("createdAt", r.getCreatedAt());
        m.put("updatedAt", r.getUpdatedAt());
        m.put("lastAttemptAt", r.getLastAttemptAt());
        m.put("nextAttemptAt", r.getNextAttemptAt() == null ? null : r.getNextAttemptAt());
        m.put("serverTime", Instant.now());
        return m;
    }
}
