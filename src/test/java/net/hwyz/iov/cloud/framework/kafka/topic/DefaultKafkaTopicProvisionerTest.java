package net.hwyz.iov.cloud.framework.kafka.topic;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * DefaultKafkaTopicProvisioner 单元测试（Mockito 模拟 Kafka Admin）。
 * 覆盖：全部已存在、部分缺失创建、TopicExists 并发幂等、部分成功部分失败、describe 故障。
 */
class DefaultKafkaTopicProvisionerTest {

    private Admin admin;
    private DefaultKafkaTopicProvisioner provisioner;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        provisioner = new DefaultKafkaTopicProvisioner(admin, Duration.ofSeconds(10));
    }

    private static TopicDescription desc(String name) {
        Node node = new Node(0, "localhost", 9092);
        TopicPartitionInfo partition = new TopicPartitionInfo(0, node, List.of(node), List.of(node));
        return new TopicDescription(name, false, List.of(partition));
    }

    @SuppressWarnings("unchecked")
    private static <T> KafkaFuture<T> failedFuture(Throwable cause) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        cf.completeExceptionally(cause);
        KafkaFuture<T> future = mock(KafkaFuture.class);
        when(future.toCompletionStage()).thenReturn(cf);
        return future;
    }

    /** 模拟 Kafka 3.x：已存在 Topic 返回成功 future，缺失 Topic 返回 UnknownTopicOrPartitionException。 */
    private static Map<String, KafkaFuture<TopicDescription>> describeValues(List<String> existing) {
        Map<String, KafkaFuture<TopicDescription>> values = new java.util.HashMap<>();
        for (String name : existing) {
            values.put(name, KafkaFuture.completedFuture(desc(name)));
        }
        return values;
    }

    @Test
    void allTopicsAlreadyExistIsSuccessWithoutCreate() throws Exception {
        KafkaTopicDefinition t1 = new KafkaTopicDefinition("t1", 3, (short) 1);
        KafkaTopicDefinition t2 = new KafkaTopicDefinition("t2", 3, (short) 1);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.values()).thenReturn(describeValues(List.of("t1", "t2")));

        KafkaTopicProvisioningResult result = provisioner.ensureTopics(List.of(t1, t2))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result.successful()).isTrue();
        assertThat(result.existingTopics()).containsExactlyInAnyOrder("t1", "t2");
        assertThat(result.createdTopics()).isEmpty();
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void createsOnlyMissingTopicsWithDeclaredDefinition() throws Exception {
        KafkaTopicDefinition existing = new KafkaTopicDefinition("existing", 3, (short) 1);
        KafkaTopicDefinition missing = new KafkaTopicDefinition("missing", 5, (short) 2,
                Map.of("cleanup.policy", "delete"));
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.values()).thenReturn(describeValues(List.of("existing")));

        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        when(admin.createTopics(anyCollection())).thenReturn(createResult);
        when(createResult.values()).thenReturn(Map.of("missing", KafkaFuture.completedFuture(null)));

        KafkaTopicProvisioningResult result = provisioner.ensureTopics(List.of(existing, missing))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result.successful()).isTrue();
        assertThat(result.createdTopics()).containsExactly("missing");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<NewTopic>> captor = ArgumentCaptor.forClass(Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic newTopic = captor.getValue().iterator().next();
        assertThat(newTopic.name()).isEqualTo("missing");
        assertThat(newTopic.numPartitions()).isEqualTo(5);
        assertThat(newTopic.replicationFactor()).isEqualTo((short) 2);
        assertThat(newTopic.configs()).containsEntry("cleanup.policy", "delete");
    }

    @Test
    void topicExistsOnConcurrentCreateIsTreatedAsSuccess() throws Exception {
        KafkaTopicDefinition t = new KafkaTopicDefinition("t", 1, (short) 1);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.values()).thenReturn(describeValues(List.of()));

        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        when(admin.createTopics(anyCollection())).thenReturn(createResult);
        Map<String, KafkaFuture<Void>> values = Map.of("t",
                failedFuture(new TopicExistsException("Topic 't' already exists")));
        when(createResult.values()).thenReturn(values);

        KafkaTopicProvisioningResult result = provisioner.ensureTopics(List.of(t))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result.successful()).isTrue();
        assertThat(result.createdTopics()).isEmpty();
        assertThat(result.existingTopics()).contains("t");
    }

    @Test
    void partialSuccessAndFailureProducesStructuredResult() throws Exception {
        KafkaTopicDefinition ok = new KafkaTopicDefinition("ok", 1, (short) 1);
        KafkaTopicDefinition bad = new KafkaTopicDefinition("bad", 1, (short) 1);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        when(describeResult.values()).thenReturn(describeValues(List.of()));

        CreateTopicsResult createResult = mock(CreateTopicsResult.class);
        when(admin.createTopics(anyCollection())).thenReturn(createResult);
        Map<String, KafkaFuture<Void>> values = Map.of(
                "ok", KafkaFuture.completedFuture(null),
                "bad", failedFuture(new InvalidReplicationFactorException("bad")));
        when(createResult.values()).thenReturn(values);

        KafkaTopicProvisioningResult result = provisioner.ensureTopics(List.of(ok, bad))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result.successful()).isFalse();
        assertThat(result.createdTopics()).containsExactly("ok");
        assertThat(result.missingTopics()).containsExactly("bad");
        assertThat(result.failures()).containsKey("bad");
        assertThat(result.failures().get("bad")).isInstanceOf(InvalidReplicationFactorException.class);
    }

    @Test
    void describeFailureProducesStructuredFailureResult() throws Exception {
        KafkaTopicDefinition t = new KafkaTopicDefinition("t", 1, (short) 1);
        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        Map<String, KafkaFuture<TopicDescription>> values = Map.of(
                "t", failedFuture(new TimeoutException("broker timeout")));
        when(describeResult.values()).thenReturn(values);

        KafkaTopicProvisioningResult result = provisioner.ensureTopics(List.of(t))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(result.successful()).isFalse();
        assertThat(result.missingTopics()).containsExactly("t");
        assertThat(result.failures().get("t")).isInstanceOf(TimeoutException.class);
        verify(admin, never()).createTopics(anyCollection());
    }

    /**
     * describe-timeout 必须真正生效：broker 不返回时按共享等待预算超时失败，
     * 而不是无限期挂在 AdminClient 默认超时上。
     */
    @Test
    void describeTimeoutIsEnforcedAsSharedBudget() throws Exception {
        DefaultKafkaTopicProvisioner briefProvisioner =
                new DefaultKafkaTopicProvisioner(admin, Duration.ofMillis(100));
        KafkaTopicDefinition t1 = new KafkaTopicDefinition("t1", 1, (short) 1);
        KafkaTopicDefinition t2 = new KafkaTopicDefinition("t2", 1, (short) 1);

        DescribeTopicsResult describeResult = mock(DescribeTopicsResult.class);
        when(admin.describeTopics(anyCollection())).thenReturn(describeResult);
        // 永不完成的 future：模拟 broker 无响应
        KafkaFuture<TopicDescription> hanging = mock(KafkaFuture.class);
        when(hanging.toCompletionStage()).thenReturn(new CompletableFuture<>());
        when(describeResult.values()).thenReturn(Map.of("t1", hanging, "t2", hanging));

        long startNanos = System.nanoTime();
        KafkaTopicProvisioningResult result = briefProvisioner.ensureTopics(List.of(t1, t2))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

        assertThat(result.successful()).isFalse();
        assertThat(result.missingTopics()).containsExactlyInAnyOrder("t1", "t2");
        assertThat(result.failures().values()).allMatch(TimeoutException.class::isInstance);
        // 共享预算：两个 Topic 不会各等一份预算
        assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void constructorRejectsInvalidArguments() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> new DefaultKafkaTopicProvisioner(null, Duration.ofSeconds(1)))
                .isInstanceOf(NullPointerException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> new DefaultKafkaTopicProvisioner(admin, null))
                .isInstanceOf(NullPointerException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> new DefaultKafkaTopicProvisioner(admin, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> new DefaultKafkaTopicProvisioner(admin, Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
