package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicProvisioningStatus;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningCoordinator;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus.State.READY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

/**
 * 端到端集成测试：自动配置装配 → Coordinator 监听 ApplicationReadyEvent → 真实 broker 创建 Topic → 状态 READY。
 * <p>broker 不可用时自动跳过。</p>
 */
class KafkaTopicProvisioningEndToEndIntegrationTest {

    private static final String BOOTSTRAP = "localhost:9094";
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
    static void close() {
        if (admin != null) {
            admin.close();
        }
    }

    @Test
    void coordinatorProvisionsTopicsAndReachesReady() throws Exception {
        assumeTrue(admin != null, "Kafka broker 不可用，跳过端到端集成测试");
        String topic = "iov-e2e-" + UUID.randomUUID();

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaTopicProvisioningAutoConfiguration.class))
                .withBean(KafkaProperties.class, () -> {
                    KafkaProperties properties = new KafkaProperties();
                    properties.setBootstrapServers(List.of(BOOTSTRAP));
                    return properties;
                })
                .withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=" + topic,
                        "iov.kafka.topic-provisioning.topics[0].partitions=1",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    assertThat(context).hasNotFailed();

                    KafkaTopicProvisioningCoordinator coordinator =
                            context.getBean(KafkaTopicProvisioningCoordinator.class);
                    context.publishEvent(new ApplicationReadyEvent(
                            mock(SpringApplication.class), new String[0],
                            context.getSourceApplicationContext(), Duration.ZERO));

                    KafkaTopicProvisioningStatus status =
                            context.getBean(DefaultKafkaTopicProvisioningStatus.class);
                    long deadline = System.currentTimeMillis() + 20_000;
                    while (status.state() != READY && System.currentTimeMillis() < deadline) {
                        Thread.sleep(200);
                    }

                    assertThat(status.state()).isEqualTo(READY);
                    assertThat(status.missingTopics()).isEmpty();
                });

        // 真实 broker 上确认 Topic 已创建
        assertThat(admin.listTopics().names().get(5, TimeUnit.SECONDS)).contains(topic);
        admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
    }
}
