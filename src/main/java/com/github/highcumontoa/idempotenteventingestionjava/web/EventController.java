package com.github.highcumontoa.idempotenteventingestionjava.web;

import com.github.highcumontoa.idempotenteventingestionjava.model.EventRecord;
import com.github.highcumontoa.idempotenteventingestionjava.model.EventRequest;
import com.github.highcumontoa.idempotenteventingestionjava.model.IngestionResponse;
import com.github.highcumontoa.idempotenteventingestionjava.model.StatsResponse;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventIngestionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 事件接入 REST 接口：
 * <ul>
 *   <li>POST /api/events：首次受理 202；重复投递 200 且 outcome=DUPLICATE；</li>
 *   <li>GET  /api/events/{key}：查询单事件状态，不存在 404；</li>
 *   <li>GET  /api/events/stats：一致性状态与统计。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventIngestionService service;

    public EventController(EventIngestionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<IngestionResponse> ingest(@RequestBody EventRequest request) {
        IngestionResponse response = service.ingest(request);
        // 首次受理异步处理 -> 202 Accepted；重复 -> 200 OK，由 outcome 字段区分
        HttpStatus status = response.getOutcome() == com.github.highcumontoa.idempotenteventingestionjava.model.IngestionOutcome.ACCEPTED
                ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status).body(response);
    }

    @GetMapping("/{idempotencyKey}")
    public ResponseEntity<EventRecord> get(@PathVariable String idempotencyKey) {
        EventRecord record = service.get(idempotencyKey);
        return record == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(record);
    }

    @GetMapping("/stats")
    public StatsResponse stats() {
        return service.stats();
    }
}
