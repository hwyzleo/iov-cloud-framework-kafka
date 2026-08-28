package net.hwyz.iov.cloud.framework.kafka.topic;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 默认状态实现：以 AtomicReference 原子替换不可变状态快照，保证线程安全读取。
 *
 * @author hwyz_leo
 */
public class DefaultKafkaTopicProvisioningStatus implements KafkaTopicProvisioningStatus {

    private final AtomicReference<Snapshot> ref = new AtomicReference<>(
            new Snapshot(State.NOT_READY, Set.of(), null, null));

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
