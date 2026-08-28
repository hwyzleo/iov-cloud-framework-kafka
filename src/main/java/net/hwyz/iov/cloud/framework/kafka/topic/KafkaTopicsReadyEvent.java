package net.hwyz.iov.cloud.framework.kafka.topic;

import java.time.Instant;

/**
 * Topic Provisioning 首次由非 READY 进入 READY 时发布的事件。
 * <p>重复成功不会重复发布状态切换事件。业务组件可通过监听本事件实现事件驱动门禁。</p>
 *
 * @author hwyz_leo
 */
public record KafkaTopicsReadyEvent(Instant occurredAt) {

    public KafkaTopicsReadyEvent() {
        this(Instant.now());
    }
}
