package net.hwyz.iov.cloud.framework.kafka.topic;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 真实 Kafka 集成测试（本地 docker broker：localhost:9094）。
 * 覆盖：创建缺失 Topic（校验分区/副本）、重复执行幂等、已存在 Topic 配置不被修改。
 * <p>broker 不可用时自动跳过，不影响无 Kafka 环境的构建。</p>
 */
class KafkaTopicProvisioningIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9094";
    private static final String TOPIC_PREFIX = "iov-it-";

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

    @Test
    void createsMissingTopicsWithDeclaredPartitionsAndReplication() throws Exception {
        String name = newTopicName();
        KafkaTopicDefinition definition = new KafkaTopicDefinition(name, 2, (short) 1,
                Map.of("cleanup.policy", "delete"));
        DefaultKafkaTopicProvisioner provisioner = new DefaultKafkaTopicProvisioner(admin);

        KafkaTopicProvisioningResult result = provisioner.ensureTopics(List.of(definition))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(result.successful()).isTrue();
        assertThat(result.createdTopics()).contains(name);

        TopicDescription description = admin.describeTopics(List.of(name)).allTopicNames()
                .get(5, TimeUnit.SECONDS).get(name);
        assertThat(description.partitions()).hasSize(2);
    }

    @Test
    void repeatedProvisioningIsIdempotent() throws Exception {
        String name = newTopicName();
        KafkaTopicDefinition definition = new KafkaTopicDefinition(name, 1, (short) 1);
        DefaultKafkaTopicProvisioner provisioner = new DefaultKafkaTopicProvisioner(admin);

        KafkaTopicProvisioningResult first = provisioner.ensureTopics(List.of(definition))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(first.createdTopics()).contains(name);

        KafkaTopicProvisioningResult second = provisioner.ensureTopics(List.of(definition))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(second.successful()).isTrue();
        assertThat(second.createdTopics()).isEmpty();
        assertThat(second.existingTopics()).contains(name);
    }

    @Test
    void existingTopicConfigIsNotModified() throws Exception {
        String name = newTopicName();
        KafkaTopicDefinition created = new KafkaTopicDefinition(name, 1, (short) 1,
                Map.of("cleanup.policy", "compact"));
        KafkaTopicDefinition altered = new KafkaTopicDefinition(name, 1, (short) 1,
                Map.of("cleanup.policy", "delete"));
        DefaultKafkaTopicProvisioner provisioner = new DefaultKafkaTopicProvisioner(admin);

        provisioner.ensureTopics(List.of(created)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        KafkaTopicProvisioningResult result = provisioner.ensureTopics(List.of(altered))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(result.successful()).isTrue();
        assertThat(result.createdTopics()).isEmpty();

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        String cleanupPolicy = admin.describeConfigs(List.of(resource)).all()
                .get(5, TimeUnit.SECONDS).get(resource).get("cleanup.policy").value();
        assertThat(cleanupPolicy).isEqualTo("compact");
    }
}
