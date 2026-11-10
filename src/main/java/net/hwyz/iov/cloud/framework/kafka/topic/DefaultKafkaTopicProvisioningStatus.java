package net.hwyz.iov.cloud.framework.kafka.topic;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 默认状态实现：以 AtomicReference 原子替换不可变状态快照，保证线程安全读取。
 *
 * <p>初始状态由构造方法决定：Provisioning 关闭时使用 {@link KafkaTopicProvisioningStatus.State#DISABLED}，
 * 启用时为 {@link KafkaTopicProvisioningStatus.State#NOT_READY}。状态 Bean 始终可注入，
 * 便于业务在功能关闭时也能按同一契约做判断。</p>
 *
 * @author hwyz_leo
 */
public class DefaultKafkaTopicProvisioningStatus implements KafkaTopicProvisioningStatus {

    private final AtomicReference<Snapshot> ref;

    /**
     * 默认以 NOT_READY 为初始状态（Provisioning 启用场景）。
     */
    public DefaultKafkaTopicProvisioningStatus() {
        this(State.NOT_READY);
    }

    /**
     * 指定初始状态。
     *
     * @param initialState 初始状态，不得为空
     */
    public DefaultKafkaTopicProvisioningStatus(State initialState) {
        this.ref = new AtomicReference<>(new Snapshot(
                Objects.requireNonNull(initialState, "initialState"), Set.of(), null, null));
    }

    private record Snapshot(State state, Set<String> missingTopics, Throwable lastFailure, Instant nextRetryAt) {
        Snapshot {
            missingTopics = Set.copyOf(missingTopics);
        }
    }

    @Override
    public State state() {
        return ref.get().state();
    }

    @Override
    public Set<String> missingTopics() {
        return ref.get().missingTopics();
    }

    @Override
    public Optional<Throwable> lastFailure() {
        return Optional.ofNullable(ref.get().lastFailure());
    }

    @Override
    public Optional<Instant> nextRetryAt() {
        return Optional.ofNullable(ref.get().nextRetryAt());
    }

    /**
     * 原子更新状态快照。
     *
     * @return 状态是否发生切换（由调用方据此决定是否发布状态切换事件）
     */
    public boolean update(State state, Set<String> missingTopics, Throwable lastFailure, Instant nextRetryAt) {
        Snapshot next = new Snapshot(state,
                missingTopics == null ? Set.of() : missingTopics, lastFailure, nextRetryAt);
        Snapshot prev = ref.getAndSet(next);
        return prev.state() != next.state();
    }
}
