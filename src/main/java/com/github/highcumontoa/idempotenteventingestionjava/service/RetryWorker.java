package com.github.highcumontoa.idempotenteventingestionjava.service;

import com.github.highcumontoa.idempotenteventingestionjava.config.EventingProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.scheduling.support.PeriodicTrigger;
import org.springframework.stereotype.Component;

/**
 * 后台重试 worker：单线程周期扫描到期事件并串行处理。
 *
 * <p>使用单一调度器（poolSize=1）确保重试不并发；事件处理本身已在存储监视器内串行化。
 * 可通过 {@code eventing.worker-enabled=false} 关闭，便于测试手工驱动处理。</p>
 */
@Component
public class RetryWorker implements SchedulingConfigurer {

    private static final Logger log = LoggerFactory.getLogger(RetryWorker.class);

    private final EventProcessingService processingService;
    private final EventingProperties properties;

    public RetryWorker(EventProcessingService processingService, EventingProperties properties) {
        this.processingService = processingService;
        this.properties = properties;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        if (!properties.isWorkerEnabled()) {
            log.info("后台重试 worker 已关闭(eventing.worker-enabled=false)");
            return;
        }
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("event-retry-worker-");
        scheduler.setDaemon(true);
        scheduler.initialize();
        registrar.setTaskScheduler(scheduler);
        PeriodicTrigger trigger = new PeriodicTrigger(properties.getWorkerInterval());
        trigger.setInitialDelay(properties.getWorkerInterval());
        registrar.addTriggerTask(this::tick, trigger);
        log.info("后台重试 worker 已启动，轮询间隔={}", properties.getWorkerInterval());
    }

    void tick() {
        try {
            processingService.processDueBatch();
        } catch (Exception e) {
            // worker 线程内绝不让异常逃逸导致调度终止。
            log.error("后台重试轮询发生异常，事件状态未受影响，下轮继续: {}", e.toString(), e);
        }
    }
}
