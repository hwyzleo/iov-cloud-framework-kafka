package net.hwyz.iov.cloud.framework.kafka.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * KafkaAdminOperations 真实 Kafka 集成测试（本地 docker broker：localhost:9094）。
 * 覆盖：warm-up 后 describe 已存在/不存在 Topic、describeConfigs。
 * <p>broker 不可用时自动跳过；本地 broker 开启 auto.create.topics.enable，
 * 缺失 Topic 的描述结果取决于 broker 配置，因此不做“必定抛 UnknownTopic”的强断言（该语义由单元测试精确覆盖）。</p>
 */
class KafkaAdminOperationsIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9094";
    private static final String TOPIC_PREFIX = "iov-it-ops-";

    private static Admin admin;

    @BeforeAll
    static void connect() {
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");
            admin = AdminClient.create(props);
            admin.describeCluster().clusterId().get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            admin = null;
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin == null) {
            return;
        }
        try {
            Set<String> topics = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            admin.deleteTopics(topics.stream().filter(n -> n.startsWith(TOPIC_PREFIX)).toList())
                    .all().get(5, TimeUnit.SECONDS);
        } finally {
            admin.close();
        }
    }

    @BeforeEach
    void assumeBrokerAvailable() {
        assumeTrue(admin != null, "Kafka broker 不可用，跳过集成测试");
    }

    private static String newTopicName() {
        return TOPIC_PREFIX + UUID.randomUUID();
    }

    /**
     * 等待 Topic 元数据在 broker 传播完成；创建后立即 describe 可能短暂报 UnknownTopicOrPartitionException。
     */
    private static void awaitTopicVisible(String name) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (admin.listTopics().names().get(2, TimeUnit.SECONDS).contains(name)) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Topic " + name + " 在 5s 内未在 broker 可见");
    }

    @Test
    void warmUpThenDescribeExistingTopic() throws Exception {
        String name = newTopicName();
        admin.createTopics(List.of(new NewTopic(name, 2, (short) 1))).all().get(5, TimeUnit.SECONDS);
        awaitTopicVisible(name);
        try {
            KafkaAdminOperations operations = new KafkaAdminOperations();

            assertThat(operations.warmUp(admin, Duration.ofSeconds(5), 2, Duration.ofMillis(100))).isTrue();

            Map<String, TopicDescription> descriptions = operations.describeTopics(
                    admin, List.of(name), Duration.ofSeconds(5));
            assertThat(descriptions.get(name)).isNotNull();
            assertThat(descriptions.get(name).partitions()).hasSize(2);
        } finally {
            admin.deleteTopics(List.of(name)).all().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void warmUpThenDescribeMissingTopic() throws Exception {
        String name = newTopicName();
        KafkaAdminOperations operations = new KafkaAdminOperations();

        assertThat(operations.warmUp(admin, Duration.ofSeconds(5), 2, Duration.ofMillis(100))).isTrue();

        Throwable thrown = catchThrowable(() ->
                operations.describeTopics(admin, List.of(name), Duration.ofSeconds(5)));
        // 未开启 auto.create：UnknownTopic 原样透出；开启时返回自动创建的 Topic 描述，均符合公共契约
        if (thrown != null) {
            assertThat(thrown).isInstanceOf(UnknownTopicOrPartitionException.class);
        }
    }

    @Test
    void describeConfigsReturnsTopicConfigs() throws Exception {
        String name = newTopicName();
        NewTopic newTopic = new NewTopic(name, 1, (short) 1);
        newTopic.configs(Map.of("cleanup.policy", "compact"));
        admin.createTopics(List.of(newTopic)).all().get(5, TimeUnit.SECONDS);
        awaitTopicVisible(name);
        try {
            KafkaAdminOperations operations = new KafkaAdminOperations();
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);

            Map<ConfigResource, org.apache.kafka.clients.admin.Config> configs = operations.describeConfigs(
                    admin, List.of(resource), Duration.ofSeconds(5));

            assertThat(configs.get(resource).get("cleanup.policy").value()).isEqualTo("compact");
        } finally {
            admin.deleteTopics(List.of(name)).all().get(5, TimeUnit.SECONDS);
        }
    }
}
