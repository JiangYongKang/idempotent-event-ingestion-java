package com.github.highcumontoa.idempotenteventingestionjava.web;

import com.github.highcumontoa.idempotenteventingestionjava.service.CapacityExceededException;
import com.github.highcumontoa.idempotenteventingestionjava.service.InvalidEventException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 统一错误反馈：
 * 非法事件 400 BAD_REQUEST；容量超限 503 SERVICE_UNAVAILABLE（限流，可稍后重试投递）。
 * 两类拒绝都给出明确原因，绝不阻塞调用或误报成功。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(InvalidEventException.class)
    public ResponseEntity<Map<String, Object>> handleInvalid(InvalidEventException ex) {
        return ResponseEntity.badRequest().body(body("INVALID_EVENT", ex.getMessage()));
    }

    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<Map<String, Object>> handleCapacity(CapacityExceededException ex) {
        Map<String, Object> body = body("CAPACITY_EXCEEDED", ex.getMessage());
        body.put("retryable", true);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(body);
    }

    private Map<String, Object> body(String error, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", error);
        m.put("message", message);
        return m;
    }
}
