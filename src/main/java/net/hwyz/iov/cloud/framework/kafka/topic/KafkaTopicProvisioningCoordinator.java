package net.hwyz.iov.cloud.framework.kafka.topic;

import net.hwyz.iov.cloud.framework.kafka.properties.TopicProvisioningProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Kafka Topic Provisioning 协调器。
 * <ul>
 *   <li>监听 {@link ApplicationReadyEvent}，通过独立 TaskScheduler 异步执行首次检查</li>
 *   <li>失败时按 min(initialInterval × multiplier^attempt, maxInterval) 指数退避持续重试</li>
 *   <li>状态原子切换；首次由非 READY 进入 READY 时经 TaskScheduler 线程投递 {@link KafkaTopicsReadyEvent}，
 *       绝不占用 Kafka AdminClient 内部线程，重复成功不重复发布</li>
 * </ul>
 *
 * @author hwyz_leo
 */
public class KafkaTopicProvisioningCoordinator implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicProvisioningCoordinator.class);

    private final KafkaTopicCatalog catalog;
    private final KafkaTopicProvisioner provisioner;
    private final DefaultKafkaTopicProvisioningStatus status;
    private final TaskScheduler taskScheduler;
    private final ApplicationEventPublisher eventPublisher;
    private final TopicProvisioningProperties properties;
    private final KafkaTopicProvisioningMetrics metrics;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicInteger attempt = new AtomicInteger(0);
    private volatile boolean running = false;

    public KafkaTopicProvisioningCoordinator(KafkaTopicCatalog catalog,
                                             KafkaTopicProvisioner provisioner,
                                             DefaultKafkaTopicProvisioningStatus status,
                                             TaskScheduler taskScheduler,
                                             ApplicationEventPublisher eventPublisher,
                                             TopicProvisioningProperties properties,
                                             KafkaTopicProvisioningMetrics metrics) {
        this.catalog = catalog;
        this.provisioner = provisioner;
        this.status = status;
        this.taskScheduler = taskScheduler;
        this.eventPublisher = eventPublisher;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if (started.compareAndSet(false, true)) {
            log.info("Kafka Topic Provisioning 已启动，调度首次执行，initialDelay={}", properties.initialDelay());
            scheduleRun(properties.initialDelay());
        }
    }

    private void scheduleRun(Duration delay) {
        taskScheduler.schedule(this::run, Instant.now().plus(delay));
    }

    private void run() {
        if (running) {
            return;
        }
        running = true;
        try {
            Collection<KafkaTopicDefinition> definitions = catalog.definitions();
            List<String> declaredTopics = definitions.stream().map(KafkaTopicDefinition::name).toList();
            log.info("开始 Kafka Topic Provisioning，attempt={}，declaredTopics={}",
                    attempt.get() + 1, declaredTopics);
            if (metrics != null) {
                metrics.recordAttempt();
            }
            provisioner.ensureTopics(definitions).whenComplete(this::handleResult);
        } catch (Exception ex) {
            handleResult(null, ex);
        }
    }

    /**
     * 处理单轮 Provisioning 结果。
     */
    void handleResult(KafkaTopicProvisioningResult result, Throwable ex) {
        running = false;
        if (ex != null || result == null) {
            fail(ex == null ? new IllegalStateException("Kafka Topic Provisioning 未返回结果") : ex,
                    declaredNames());
            return;
        }
        if (result.successful()) {
            succeed(result);
        } else {
            fail(firstFailure(result.failures()), result.missingTopics());
        }
    }

    private void succeed(KafkaTopicProvisioningResult result) {
        if (metrics != null) {
            metrics.recordCreated(result.createdTopics().size());
        }
        boolean changed = status.update(KafkaTopicProvisioningStatus.State.READY, Set.of(), null, null);
        log.info("Kafka Topic Provisioning 成功，createdTopics={}", result.createdTopics());
        if (changed) {
            // 状态先原子切换，事件再经独立 TaskScheduler 投递：
            // handleResult 的完成回调运行在 Kafka AdminClient 内部线程上，
            // 若在此直接 publishEvent，任何阻塞型监听器都会卡住 Admin 网络线程，
            // 影响该 Admin 实例上全部异步管理操作。改经调度线程发布可从根本上规避。
            taskScheduler.schedule(() -> {
                eventPublisher.publishEvent(new KafkaTopicsReadyEvent());
                log.info("Kafka Topic Provisioning 已就绪，发布 KafkaTopicsReadyEvent");
            }, Instant.now());
        }
        attempt.set(0);
    }

    private void fail(Throwable failure, Set<String> missingTopics) {
        int attemptNo = attempt.incrementAndGet();
        Duration delay = backoff(attemptNo);
        Instant nextRetryAt = Instant.now().plus(delay);
        status.update(KafkaTopicProvisioningStatus.State.NOT_READY, missingTopics, failure, nextRetryAt);
        if (metrics != null) {
            metrics.recordFailure();
        }
        log.warn("Kafka Topic Provisioning 未就绪，missingTopics={}，exceptionClass={}，nextRetryAt={}",
                missingTopics, failure == null ? null : failure.getClass().getSimpleName(), nextRetryAt);
        scheduleRun(delay);
    }

    /**
     * 指数退避：min(initialInterval × multiplier^(attempt-1), maxInterval)。
     */
    Duration backoff(int attemptNo) {
        Duration initial = properties.retry().initialInterval();
        Duration max = properties.retry().maxInterval();
        if (attemptNo <= 1) {
            return initial.compareTo(max) <= 0 ? initial : max;
        }
        double value = initial.toMillis() * Math.pow(properties.retry().multiplier(), attemptNo - 1);
        if (value >= max.toMillis()) {
            return max;
        }
        return Duration.ofMillis((long) value);
    }

    private Set<String> declaredNames() {
        return catalog.definitions().stream()
                .map(KafkaTopicDefinition::name)
                .collect(Collectors.toSet());
    }

    private Throwable firstFailure(java.util.Map<String, Throwable> failures) {
        return failures.isEmpty() ? null : failures.values().iterator().next();
    }
}
