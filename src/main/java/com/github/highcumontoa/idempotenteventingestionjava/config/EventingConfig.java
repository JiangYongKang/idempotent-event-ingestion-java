package com.github.highcumontoa.idempotenteventingestionjava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 事件处理模块配置：注册配置项与事件处理线程池。
 */
@Configuration
@EnableConfigurationProperties(EventingProperties.class)
public class EventingConfig {

    /**
     * 事件执行线程池：有界队列 + CallerRunsPolicy，
     * 容量在服务层另有准入控制，线程池满时退化为调用方执行而非误报成功。
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService eventTaskExecutor(EventingProperties properties) {
        return new ThreadPoolExecutor(
                properties.getWorkerThreads(),
                properties.getWorkerThreads(),
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(properties.getMaxInFlight()),
                r -> {
                    Thread t = new Thread(r, "event-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
