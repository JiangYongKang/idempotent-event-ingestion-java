package com.github.highcumontoa.idempotenteventingestionjava.service;

/**
 * 超出处理容量：明确限流，不阻塞、不误报成功。
 */
public class CapacityExceededException extends RuntimeException {
    public CapacityExceededException(String message) {
        super(message);
    }
}
