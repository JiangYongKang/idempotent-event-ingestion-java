package com.github.highcumontoa.idempotenteventingestionjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 事件处理相关可配置项（前缀 eventing）。
 */
@ConfigurationProperties(prefix = "eventing")
public class EventingProperties {

    /** 事件存储目录（本地文件，不依赖外部服务）。 */
    private String dataDir = "data/eventing";
    /** 最多容纳的未完成事件（PENDING+PROCESSING）容量，超出则限流拒绝。 */
    private int maxInFlight = 1000;
    /** 最大重试次数（不含首次尝试）。 */
    private int maxRetries = 3;
    /** 指数退避初始延迟毫秒。 */
    private long backoffInitialMillis = 200;
    /** 退避倍数。 */
    private double backoffMultiplier = 2.0;
    /** 退避上限毫秒。 */
    private long backoffMaxMillis = 5000;
    /** 重试扫描间隔毫秒。 */
    private long schedulerIntervalMillis = 100;
    /** 立即处理线程池大小。 */
    private int workerThreads = 4;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public int getMaxInFlight() { return maxInFlight; }
    public void setMaxInFlight(int maxInFlight) { this.maxInFlight = maxInFlight; }
    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public long getBackoffInitialMillis() { return backoffInitialMillis; }
    public void setBackoffInitialMillis(long backoffInitialMillis) { this.backoffInitialMillis = backoffInitialMillis; }
    public double getBackoffMultiplier() { return backoffMultiplier; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public long getBackoffMaxMillis() { return backoffMaxMillis; }
    public void setBackoffMaxMillis(long backoffMaxMillis) { this.backoffMaxMillis = backoffMaxMillis; }
    public long getSchedulerIntervalMillis() { return schedulerIntervalMillis; }
    public void setSchedulerIntervalMillis(long schedulerIntervalMillis) { this.schedulerIntervalMillis = schedulerIntervalMillis; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
}
