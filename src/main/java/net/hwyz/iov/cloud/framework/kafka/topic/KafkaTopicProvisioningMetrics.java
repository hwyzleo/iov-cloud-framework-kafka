package net.hwyz.iov.cloud.framework.kafka.topic;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Topic Provisioning 可观测性指标（Micrometer 可用时条件化装配）。
 * <ul>
 *   <li>Gauge：iov.kafka.topic.provisioning.ready（READY 为 1，否则 0）</li>
 *   <li>Counter：iov.kafka.topic.provisioning.attempt / failure / created</li>
 * </ul>
 *
 * @author hwyz_leo
 */
public class KafkaTopicProvisioningMetrics {

    private final Counter attempts;
    private final Counter failures;
    private final Counter created;

    public KafkaTopicProvisioningMetrics(MeterRegistry registry, KafkaTopicProvisioningStatus status) {
        Gauge.builder("iov.kafka.topic.provisioning.ready", status,
                        s -> s.state() == KafkaTopicProvisioningStatus.State.READY ? 1.0 : 0.0)
                .description("Kafka Topic Provisioning 是否就绪")
                .register(registry);
        attempts = Counter.builder("iov.kafka.topic.provisioning.attempt")
                .description("Kafka Topic Provisioning 尝试次数")
                .register(registry);
        failures = Counter.builder("iov.kafka.topic.provisioning.failure")
                .description("Kafka Topic Provisioning 失败次数")
                .register(registry);
        created = Counter.builder("iov.kafka.topic.provisioning.created")
                .description("Kafka Topic Provisioning 创建的 Topic 数量")
                .register(registry);
    }

    public void recordAttempt() {
        attempts.increment();
    }

    public void recordFailure() {
        failures.increment();
    }

    public void recordCreated(int count) {
        created.increment(count);
    }
}
