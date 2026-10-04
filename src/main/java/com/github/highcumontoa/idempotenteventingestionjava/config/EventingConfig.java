package com.github.highcumontoa.idempotenteventingestionjava.config;

import com.github.highcumontoa.idempotenteventingestionjava.handler.DemoLedgerEventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.handler.EventHandler;
import com.github.highcumontoa.idempotenteventingestionjava.service.EventProcessingService;
import com.github.highcumontoa.idempotenteventingestionjava.service.RetryBackoffPolicy;
import com.github.highcumontoa.idempotenteventingestionjava.store.EventStore;
import com.github.highcumontoa.idempotenteventingestionjava.store.FileEventStore;
import java.nio.file.Path;
import java.time.Clock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 本地装配：全部组件都在进程内运行，不依赖任何外部服务或真实凭据。
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(EventingProperties.class)
public class EventingConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public EventHandler eventHandler() {
        return new DemoLedgerEventHandler();
    }

    @Bean
    public RetryBackoffPolicy retryBackoffPolicy(EventingProperties props) {
        return new RetryBackoffPolicy(props.getMaxAttempts(), props.getRetryBaseDelay(),
                props.getRetryMaxDelay(), props.getRetryMultiplier());
    }

    @Bean
    public EventStore eventStore(EventingProperties props, Clock clock) {
        return new FileEventStore(Path.of(props.getDataFile()), clock);
    }

    @Bean
    public EventProcessingService eventProcessingService(EventStore store, EventHandler handler,
                                                         RetryBackoffPolicy policy, Clock clock) {
        return new EventProcessingService(store, handler, policy, clock);
    }
}
