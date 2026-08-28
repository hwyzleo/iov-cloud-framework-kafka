package net.hwyz.iov.cloud.framework.kafka.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

/**
 * iov.kafka.topic-provisioning 配置属性模型。
 * <p>Admin 连接、认证、SSL/SASL 等参数复用 Spring Boot KafkaProperties。</p>
 *
 * @param enabled       是否启用 Kafka Topic Provisioning，默认 false
 * @param initialDelay  应用就绪后的首次执行延迟，默认 0s
 * @param retry         失败重试策略
 * @param topics        声明式 Topic 定义列表，可由 application 配置或 Nacos 提供
 * @author hwyz_leo
 */
@ConfigurationProperties(prefix = "iov.kafka.topic-provisioning")
public record TopicProvisioningProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue("0s") Duration initialDelay,
        Retry retry,
        List<KafkaTopicDefinitionProperties> topics) {

    public TopicProvisioningProperties {
        retry = retry == null ? new Retry(Duration.ofSeconds(5), Duration.ofSeconds(60), 2.0) : retry;
        topics = topics == null ? List.of() : List.copyOf(topics);
    }

    /**
     * 指数退避重试策略。
     *
     * @param initialInterval 首次重试间隔，默认 5s
     * @param maxInterval     最大重试间隔（封顶），默认 60s
     * @param multiplier      退避倍率，默认 2.0
     */
    public record Retry(
            @DefaultValue("5s") Duration initialInterval,
            @DefaultValue("60s") Duration maxInterval,
            @DefaultValue("2.0") double multiplier) {

        public Retry {
            initialInterval = initialInterval == null ? Duration.ofSeconds(5) : initialInterval;
            maxInterval = maxInterval == null ? Duration.ofSeconds(60) : maxInterval;
        }
    }
}
