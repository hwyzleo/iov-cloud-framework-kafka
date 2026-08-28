package net.hwyz.iov.cloud.framework.kafka.topic;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;

/**
 * Kafka Topic Provisioning 状态，业务组件可安全读取以决定是否继续消息发送等操作。
 * <p>实现使用 AtomicReference 原子替换不可变状态快照，读取无需加锁。</p>
 *
 * @author hwyz_leo
 */
public interface KafkaTopicProvisioningStatus {

    /**
     * Provisioning 状态。
     */
    enum State {
        /**
         * 未启用（enabled=false）。启用场景下状态 Bean 以 NOT_READY 起步。
         */
        DISABLED,
        /**
         * 未就绪：存在缺失 Topic 或最近一次检查失败，等待后台重试。
         */
        NOT_READY,
        /**
         * 就绪：全部声明 Topic 已存在或创建成功。
         */
        READY
    }

    /**
     * 当前状态。
     */
    State state();

    /**
     * 当前仍缺失的 Topic 名称集合（不可变）。
     */
    Set<String> missingTopics();

    /**
     * 最近一次失败的异常（如有）。
     */
    Optional<Throwable> lastFailure();

    /**
     * 下一次自动重试时间（如有）。
     */
    Optional<Instant> nextRetryAt();
}
