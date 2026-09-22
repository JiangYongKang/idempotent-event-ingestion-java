package com.github.highcumontoa.idempotenteventingestionjava.service;

/**
 * 非法事件（如幂等键缺失），拒绝且不计入正常处理。
 */
public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message) {
        super(message);
    }
}
