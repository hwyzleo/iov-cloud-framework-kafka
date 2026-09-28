package net.hwyz.iov.cloud.framework.kafka.topic;

import net.hwyz.iov.cloud.framework.kafka.properties.TopicProvisioningProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus.State.NOT_READY;
import static net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus.State.READY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * KafkaTopicProvisioningCoordinator 单元测试：
 * 状态原子切换、READY 事件仅首次发布、指数退避封顶、broker 恢复自动 READY（F3）。
 */
class KafkaTopicProvisioningCoordinatorTest {

    private KafkaTopicCatalog catalog;
    private KafkaTopicProvisioner provisioner;
    private TaskScheduler taskScheduler;
    private ApplicationEventPublisher eventPublisher;
    private TopicProvisioningProperties properties;
    private DefaultKafkaTopicProvisioningStatus status;
    private KafkaTopicProvisioningCoordinator coordinator;

    @BeforeEach
    void setUp() {
        catalog = mock(KafkaTopicCatalog.class);
        provisioner = mock(KafkaTopicProvisioner.class);
        taskScheduler = mock(TaskScheduler.class);
        eventPublisher = mock(ApplicationEventPublisher.class);
        properties = defaultProperties();
        status = new DefaultKafkaTopicProvisioningStatus();
        coordinator = newCoordinator(properties);
        when(catalog.definitions()).thenReturn(List.of(new KafkaTopicDefinition("t", 1, (short) 1)));
    }

    private TopicProvisioningProperties defaultProperties() {
        return new TopicProvisioningProperties(false, Duration.ZERO,
                Duration.ofSeconds(10),
                new TopicProvisioningProperties.Warmup(true, 3, Duration.ofSeconds(1)),
                new TopicProvisioningProperties.Retry(Duration.ofSeconds(5), Duration.ofSeconds(60), 2.0),
                List.of());
    }

    private KafkaTopicProvisioningCoordinator newCoordinator(TopicProvisioningProperties properties) {
        return new KafkaTopicProvisioningCoordinator(catalog, provisioner, status,
                taskScheduler, eventPublisher, properties, null);
    }

    @Test
    void successTransitionsToReadyAndPublishesEventOnlyOnFirstTransition() {
        KafkaTopicProvisioningResult success = KafkaTopicProvisioningResult.success(Set.of("t"), Set.of());

        coordinator.handleResult(success, null);
        assertThat(status.state()).isEqualTo(READY);
        assertThat(status.missingTopics()).isEmpty();
        verify(eventPublisher, times(1)).publishEvent(any(KafkaTopicsReadyEvent.class));

        // 重复成功不重复发布状态切换事件
        coordinator.handleResult(success, null);
        assertThat(status.state()).isEqualTo(READY);
        verify(eventPublisher, times(1)).publishEvent(any(KafkaTopicsReadyEvent.class));
    }

    @Test
    void failureTransitionsToNotReadyAndSchedulesRetry() {
        KafkaTopicProvisioningResult failure = KafkaTopicProvisioningResult.failure(
                Set.of(), Set.of(), Set.of("t"), Map.of("t", new IllegalStateException("boom")));

        coordinator.handleResult(failure, null);

        assertThat(status.state()).isEqualTo(NOT_READY);
        assertThat(status.missingTopics()).contains("t");
        assertThat(status.lastFailure()).isPresent();
        assertThat(status.nextRetryAt()).isPresent();
        verify(taskScheduler).schedule(any(Runnable.class), any(Instant.class));
        verify(eventPublisher, never()).publishEvent(any(KafkaTopicsReadyEvent.class));
    }

    @Test
    void exceptionalCompletionTransitionsToNotReady() {
        coordinator.handleResult(null, new RuntimeException("broker down"));

        assertThat(status.state()).isEqualTo(NOT_READY);
        assertThat(status.missingTopics()).contains("t");
        assertThat(status.lastFailure()).isPresent();
        verify(taskScheduler).schedule(any(Runnable.class), any(Instant.class));
    }

    @Test
    void recoversFromNotReadyToReadyAfterBrokerRestored() {
        // broker 不可用 → NOT_READY
        coordinator.handleResult(null, new RuntimeException("broker down"));
        assertThat(status.state()).isEqualTo(NOT_READY);
        verify(taskScheduler, times(1)).schedule(any(Runnable.class), any(Instant.class));

        // broker 恢复 → 自动 READY 并发布一次事件
        KafkaTopicProvisioningResult success = KafkaTopicProvisioningResult.success(Set.of("t"), Set.of());
        coordinator.handleResult(success, null);
        assertThat(status.state()).isEqualTo(READY);
        assertThat(status.missingTopics()).isEmpty();
        verify(eventPublisher, times(1)).publishEvent(any(KafkaTopicsReadyEvent.class));
    }

    @Test
    void backoffIsExponentialAndCappedAtMaxInterval() {
        properties = new TopicProvisioningProperties(false, Duration.ZERO,
                Duration.ofSeconds(10),
                new TopicProvisioningProperties.Warmup(true, 3, Duration.ofSeconds(1)),
                new TopicProvisioningProperties.Retry(Duration.ofSeconds(1), Duration.ofSeconds(4), 2.0),
                List.of());
        coordinator = newCoordinator(properties);

        assertThat(coordinator.backoff(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(coordinator.backoff(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(coordinator.backoff(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(coordinator.backoff(10)).isEqualTo(Duration.ofSeconds(4)); // 封顶
    }

    @Test
    void backoffRespectsInitialIntervalWhenMaxIsSmaller() {
        properties = new TopicProvisioningProperties(false, Duration.ZERO,
                Duration.ofSeconds(10),
                new TopicProvisioningProperties.Warmup(true, 3, Duration.ofSeconds(1)),
                new TopicProvisioningProperties.Retry(Duration.ofSeconds(10), Duration.ofSeconds(2), 2.0),
                List.of());
        coordinator = newCoordinator(properties);

        assertThat(coordinator.backoff(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(coordinator.backoff(5)).isEqualTo(Duration.ofSeconds(2));
    }
}
