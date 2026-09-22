package com.github.highcumontoa.idempotenteventingestionjava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 后台重试调度器：
 * <ol>
 *   <li>应用就绪后先执行重启恢复（PROCESSING -> PENDING），再统一调度；</li>
 *   <li>之后以固定间隔扫描退避到期的 PENDING 事件重新提交。</li>
 * </ol>
 */
@Component
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final EventIngestionService service;

    public RetryScheduler(EventIngestionService service) {
        this.service = service;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        int recovered = service.recover();
        int dispatched = service.dispatchDueEvents();
        log.info("启动调度完成 判定依据=恢复{}个中断事件,立即调度{}个待处理事件", recovered, dispatched);
    }

    @Scheduled(fixedDelayString = "${eventing.scheduler-interval-millis:100}")
    public void tick() {
        try {
            service.dispatchDueEvents();
        } catch (Exception e) {
            // 调度异常不得中断后续轮询
            log.warn("重试调度轮询异常(不影响下一轮): {}", e.toString());
        }
    }
}
