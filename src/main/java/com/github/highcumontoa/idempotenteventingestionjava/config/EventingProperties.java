package com.github.highcumontoa.idempotenteventingestionjava.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 事件接入与重试的可配置项（前缀 {@code eventing}）。
 */
@ConfigurationProperties(prefix = "eventing")
public class EventingProperties {

    /** 本地持久化数据文件路径（Java 序列化）。 */
    private String dataFile = "data/events.dat";

    /** 最多尝试次数（含首次处理）。达到上限后仍失败则终止。 */
    private int maxAttempts = 5;

    /** 指数退避基础延迟。 */
    private Duration retryBaseDelay = Duration.ofMillis(200);

    /** 退避延迟上限。 */
    private Duration retryMaxDelay = Duration.ofSeconds(10);

    /** 退避乘子。 */
    private double retryMultiplier = 2.0;

    /** 排队（QUEUED）容量上限；超过则以 503 限流拒绝，绝不阻塞调用方。 */
    private int maxQueueCapacity = 1000;

    /** 同一事件允许的最大重跑轮数；到顶后再发起重跑将以 RERUN_LIMIT_REACHED 明确拒绝。 */
    private int maxReruns = 3;

    /** 幂等键最大长度。 */
    private int maxKeyLength = 128;

    /** 载荷最大字节数（按 UTF-8 计）。 */
    private int maxPayloadBytes = 65536;

    /** 后台重试轮询间隔。 */
    private Duration workerInterval = Duration.ofMillis(100);

    /** 是否启用后台重试 worker（测试可关闭后手工驱动）。 */
    private boolean workerEnabled = true;

    public String getDataFile() {
        return dataFile;
    }

    public void setDataFile(String dataFile) {
        this.dataFile = dataFile;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getRetryBaseDelay() {
        return retryBaseDelay;
    }

    public void setRetryBaseDelay(Duration retryBaseDelay) {
        this.retryBaseDelay = retryBaseDelay;
    }

    public Duration getRetryMaxDelay() {
        return retryMaxDelay;
    }

    public void setRetryMaxDelay(Duration retryMaxDelay) {
        this.retryMaxDelay = retryMaxDelay;
    }

    public double getRetryMultiplier() {
        return retryMultiplier;
    }

    public void setRetryMultiplier(double retryMultiplier) {
        this.retryMultiplier = retryMultiplier;
    }

    public int getMaxQueueCapacity() {
        return maxQueueCapacity;
    }

    public void setMaxQueueCapacity(int maxQueueCapacity) {
        this.maxQueueCapacity = maxQueueCapacity;
    }

    public int getMaxReruns() {
        return maxReruns;
    }

    public void setMaxReruns(int maxReruns) {
        this.maxReruns = maxReruns;
    }

    public int getMaxKeyLength() {
        return maxKeyLength;
    }

    public void setMaxKeyLength(int maxKeyLength) {
        this.maxKeyLength = maxKeyLength;
    }

    public int getMaxPayloadBytes() {
        return maxPayloadBytes;
    }

    public void setMaxPayloadBytes(int maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public Duration getWorkerInterval() {
        return workerInterval;
    }

    public void setWorkerInterval(Duration workerInterval) {
        this.workerInterval = workerInterval;
    }

    public boolean isWorkerEnabled() {
        return workerEnabled;
    }

    public void setWorkerEnabled(boolean workerEnabled) {
        this.workerEnabled = workerEnabled;
    }
}
