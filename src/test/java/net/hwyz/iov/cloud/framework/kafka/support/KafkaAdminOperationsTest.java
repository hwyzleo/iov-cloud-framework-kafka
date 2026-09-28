package net.hwyz.iov.cloud.framework.kafka.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * KafkaAdminOperations 单元测试（Mockito 模拟 Kafka Admin）。
 * 覆盖：describeTopics/describeConfigs 正常、去重保序、空集合、空 Admin、非法参数、共享 deadline 超时、
 * 异常解包与透出、线程中断；warmUp 首次成功、失败后恢复、耗尽、非法参数、退避中断，以及日志级别。
 */
class KafkaAdminOperationsTest {

    private Admin admin;
    private DescribeClusterResult clusterResult;
    private KafkaAdminOperations operations;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        clusterResult = mock(DescribeClusterResult.class);
        operations = new KafkaAdminOperations();
        when(admin.describeCluster()).thenReturn(clusterResult);
    }

    private static TopicDescription desc(String name) {
        Node node = new Node(0, "localhost", 9092);
        TopicPartitionInfo partition = new TopicPartitionInfo(0, node, List.of(node), List.of(node));
        return new TopicDescription(name, false, List.of(partition));
    }

    @SuppressWarnings("unchecked")
    private static <T> KafkaFuture<T> completedFuture(T value) throws Exception {
        KafkaFuture<T> future = mock(KafkaFuture.class);
        when(future.get(anyLong(), any(TimeUnit.class))).thenReturn(value);
        return future;
    }

    @SuppressWarnings("unchecked")
    private static <T> KafkaFuture<T> failedFuture(Throwable cause) throws Exception {
        KafkaFuture<T> future = mock(KafkaFuture.class);
        when(future.get(anyLong(), any(TimeUnit.class))).thenThrow(new ExecutionException(cause));
        return future;
    }

    // ---------- describeTopics ----------

    @Test
    void describeTopicsDeduplicatesAndPreservesInputOrder() throws Exception {
        KafkaFuture<TopicDescription> futureA = completedFuture(desc("a"));
        KafkaFuture<TopicDescription> futureB = completedFuture(desc("b"));
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.topicNameValues()).thenReturn(Map.of("a", futureA, "b", futureB));

        Map<String, TopicDescription> result = operations.describeTopics(
                admin, List.of("b", "a", "b"), Duration.ofSeconds(5));

        assertThat(result.keySet()).containsExactly("b", "a");
        assertThat(result.get("a").name()).isEqualTo("a");
        assertThat(result.get("b").name()).isEqualTo("b");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<String>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).describeTopics(captor.capture());
        assertThat(captor.getValue()).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void describeTopicsWithEmptyCollectionReturnsEmptyMapWithoutAdminCall() throws Exception {
        Map<String, TopicDescription> result = operations.describeTopics(admin, List.of(), Duration.ofSeconds(5));

        assertThat(result).isEmpty();
        verify(admin, never()).describeTopics(anyCollection());
    }

    @Test
    void describeTopicsWithNullAdminFailsFast() {
        assertThatThrownBy(() -> operations.describeTopics(null, List.of("t"), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void describeTopicsWithInvalidTimeoutFailsFast() {
        assertThatThrownBy(() -> operations.describeTopics(admin, List.of("t"), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> operations.describeTopics(admin, List.of("t"), Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> operations.describeTopics(admin, List.of("t"), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void describeTopicsWithBlankNameFailsFast() {
        assertThatThrownBy(() -> operations.describeTopics(admin, List.of(" "), Duration.ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void describeTopicsTimeoutProducesContextualMessage() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaFuture<TopicDescription> slow = mock(KafkaFuture.class);
        when(slow.get(anyLong(), any(TimeUnit.class))).thenThrow(new TimeoutException("broker slow"));
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.topicNameValues()).thenReturn(Map.of("t", slow));

        Throwable thrown = catchThrowable(() ->
                operations.describeTopics(admin, List.of("t"), Duration.ofSeconds(5)));

        assertThat(thrown).isInstanceOf(TimeoutException.class)
                .hasMessageContaining("describeTopics")
                .hasMessageContaining("targetCount=1")
                .hasMessageContaining("target=t")
                .hasMessageContaining("PT5S")
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void describeTopicsThrowsWhenRemainingBudgetExhaustedForLaterTarget() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaFuture<TopicDescription> slow = mock(KafkaFuture.class);
        when(slow.get(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            Thread.sleep(200);
            return desc("a");
        });
        KafkaFuture<TopicDescription> quick = completedFuture(desc("b"));
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.topicNameValues()).thenReturn(Map.of("a", slow, "b", quick));

        assertThatThrownBy(() -> operations.describeTopics(admin, List.of("a", "b"), Duration.ofMillis(50)))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("describeTopics")
                .hasMessageContaining("target=b");
    }

    @Test
    void describeTopicsPassesThroughUnknownTopicOrPartitionException() throws Exception {
        UnknownTopicOrPartitionException original = new UnknownTopicOrPartitionException("no such topic");
        KafkaFuture<TopicDescription> failed = failedFuture(original);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.topicNameValues()).thenReturn(Map.of("t", failed));

        Throwable thrown = catchThrowable(() ->
                operations.describeTopics(admin, List.of("t"), Duration.ofSeconds(5)));

        assertThat(thrown).isInstanceOf(UnknownTopicOrPartitionException.class).isSameAs(original);
    }

    @Test
    void describeTopicsUnwrapsNestedExecutionAndCompletionExceptions() throws Exception {
        IllegalStateException root = new IllegalStateException("boom");
        CompletionException completion = new CompletionException(root);
        ExecutionException execution = new ExecutionException(completion);
        KafkaFuture<TopicDescription> failed = failedFuture(execution);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.topicNameValues()).thenReturn(Map.of("t", failed));

        Throwable thrown = catchThrowable(() ->
                operations.describeTopics(admin, List.of("t"), Duration.ofSeconds(5)));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).isSameAs(root);
    }

    @Test
    void describeTopicsWrapsOtherCheckedExceptionsInIllegalStateException() throws Exception {
        IOException root = new IOException("io error");
        KafkaFuture<TopicDescription> failed = failedFuture(root);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.topicNameValues()).thenReturn(Map.of("t", failed));

        Throwable thrown = catchThrowable(() ->
                operations.describeTopics(admin, List.of("t"), Duration.ofSeconds(5)));

        assertThat(thrown).isInstanceOf(IllegalStateException.class).hasCause(root);
    }

    @Test
    void describeTopicsInterruptedRestoresInterruptFlag() throws Exception {
        @SuppressWarnings("unchecked")
        KafkaFuture<TopicDescription> interrupted = mock(KafkaFuture.class);
        when(interrupted.get(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("interrupted"));
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.topicNameValues()).thenReturn(Map.of("t", interrupted));

        Thread.currentThread().interrupt();
        try {
            Throwable thrown = catchThrowable(() ->
                    operations.describeTopics(admin, List.of("t"), Duration.ofSeconds(5)));
            assertThat(thrown).isInstanceOf(IllegalStateException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    // ---------- describeConfigs ----------

    @Test
    void describeConfigsReturnsConfigsForResources() throws Exception {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "t");
        ConfigResource broker = new ConfigResource(ConfigResource.Type.BROKER, "0");
        Config topicConfig = new Config(List.of(new ConfigEntry("cleanup.policy", "delete")));
        Config brokerConfig = new Config(List.of(new ConfigEntry("num.partitions", "1")));
        KafkaFuture<Config> topicFuture = completedFuture(topicConfig);
        KafkaFuture<Config> brokerFuture = completedFuture(brokerConfig);
        DescribeConfigsResult configResult = mock(DescribeConfigsResult.class);
        when(admin.describeConfigs(anyCollection())).thenReturn(configResult);
        when(configResult.values()).thenReturn(Map.of(topic, topicFuture, broker, brokerFuture));

        Map<ConfigResource, Config> result = operations.describeConfigs(
                admin, List.of(topic, broker), Duration.ofSeconds(5));

        assertThat(result).containsEntry(topic, topicConfig).containsEntry(broker, brokerConfig);
        verify(admin, times(1)).describeConfigs(anyCollection());
    }

    @Test
    void describeConfigsWithEmptyCollectionReturnsEmptyMapWithoutAdminCall() throws Exception {
        Map<ConfigResource, Config> result = operations.describeConfigs(admin, List.of(), Duration.ofSeconds(5));

        assertThat(result).isEmpty();
        verify(admin, never()).describeConfigs(anyCollection());
    }

    @Test
    void describeConfigsSharesDeadlineAcrossResources() throws Exception {
        ConfigResource a = new ConfigResource(ConfigResource.Type.TOPIC, "a");
        ConfigResource b = new ConfigResource(ConfigResource.Type.TOPIC, "b");
        @SuppressWarnings("unchecked")
        KafkaFuture<Config> slow = mock(KafkaFuture.class);
        when(slow.get(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            Thread.sleep(200);
            return new Config(List.of());
        });
        KafkaFuture<Config> quick = completedFuture(new Config(List.of()));
        DescribeConfigsResult configResult = mock(DescribeConfigsResult.class);
        when(admin.describeConfigs(anyCollection())).thenReturn(configResult);
        when(configResult.values()).thenReturn(Map.of(a, slow, b, quick));

        assertThatThrownBy(() -> operations.describeConfigs(admin, List.of(a, b), Duration.ofMillis(50)))
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("describeConfigs")
                .hasMessageContaining("name='b'");
    }

    // ---------- warmUp ----------

    @Test
    void warmUpSucceedsOnFirstAttempt() throws Exception {
        KafkaFuture<String> success = completedFuture("cluster-1");
        when(clusterResult.clusterId()).thenReturn(success);

        boolean result = operations.warmUp(admin, Duration.ofSeconds(5), 3, Duration.ofSeconds(1));

        assertThat(result).isTrue();
        verify(admin, times(1)).describeCluster();
    }

    @Test
    void warmUpRecoversAfterTransientFailure() throws Exception {
        KafkaFuture<String> failed = failedFuture(new TimeoutException("cold start"));
        KafkaFuture<String> success = completedFuture("cluster-1");
        when(clusterResult.clusterId()).thenReturn(failed, success);

        boolean result = operations.warmUp(admin, Duration.ofSeconds(5), 3, Duration.ofMillis(1));

        assertThat(result).isTrue();
        verify(admin, times(2)).describeCluster();
    }

    @Test
    void warmUpReturnsFalseWhenAllAttemptsExhausted() throws Exception {
        KafkaFuture<String> failed = failedFuture(new IllegalStateException("down"));
        when(clusterResult.clusterId()).thenReturn(failed);

        boolean result = operations.warmUp(admin, Duration.ofSeconds(5), 3, Duration.ofMillis(1));

        assertThat(result).isFalse();
        verify(admin, times(3)).describeCluster();
    }

    @Test
    void warmUpRejectsInvalidArguments() {
        assertThatThrownBy(() -> operations.warmUp(admin, Duration.ofSeconds(5), 0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> operations.warmUp(admin, Duration.ZERO, 3, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> operations.warmUp(admin, Duration.ofSeconds(5), 3, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> operations.warmUp(null, Duration.ofSeconds(5), 3, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void warmUpInterruptedDuringBackoffReturnsFalseAndRestoresInterruptFlag() throws Exception {
        KafkaFuture<String> failed = failedFuture(new IllegalStateException("down"));
        when(clusterResult.clusterId()).thenReturn(failed);

        Thread.currentThread().interrupt();
        try {
            boolean result = operations.warmUp(admin, Duration.ofSeconds(5), 3, Duration.ofMillis(1));
            assertThat(result).isFalse();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void warmUpExhaustedLogsWarnWithoutError() throws Exception {
        KafkaFuture<String> failed = failedFuture(new IllegalStateException("down"));
        when(clusterResult.clusterId()).thenReturn(failed);
        ListAppender appender = attachAppender();
        try {
            boolean result = operations.warmUp(admin, Duration.ofSeconds(5), 3, Duration.ofMillis(1));

            assertThat(result).isFalse();
            assertThat(appender.events()).extracting(LogEvent::getLevel)
                    .contains(Level.WARN)
                    .doesNotContain(Level.ERROR);
        } finally {
            detachAppender(appender);
        }
    }

    @Test
    void warmUpRecoveryLogsInfoAfterFailure() throws Exception {
        KafkaFuture<String> failed = failedFuture(new IllegalStateException("down"));
        KafkaFuture<String> success = completedFuture("cluster-1");
        when(clusterResult.clusterId()).thenReturn(failed, success);
        ListAppender appender = attachAppender();
        try {
            boolean result = operations.warmUp(admin, Duration.ofSeconds(5), 3, Duration.ofMillis(1));

            assertThat(result).isTrue();
            assertThat(appender.events()).extracting(LogEvent::getLevel)
                    .contains(Level.WARN, Level.INFO)
                    .doesNotContain(Level.ERROR);
        } finally {
            detachAppender(appender);
        }
    }

    // ---------- log4j2 日志捕获 ----------

    private static final class ListAppender extends AbstractAppender {

        private final List<LogEvent> events = new ArrayList<>();

        ListAppender() {
            super("KafkaAdminOperationsTestAppender", null, null, true, Property.EMPTY_ARRAY);
            start();
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        List<LogEvent> events() {
            return events;
        }
    }

    private Level originalLevel;

    private ListAppender attachAppender() {
        Logger logger = (Logger) LogManager.getLogger(KafkaAdminOperations.class);
        originalLevel = logger.getLevel();
        ListAppender appender = new ListAppender();
        logger.addAppender(appender);
        logger.setLevel(Level.DEBUG);
        return appender;
    }

    private void detachAppender(ListAppender appender) {
        Logger logger = (Logger) LogManager.getLogger(KafkaAdminOperations.class);
        logger.removeAppender(appender);
        logger.setLevel(originalLevel);
    }
}
