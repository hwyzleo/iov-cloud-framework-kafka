package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import net.hwyz.iov.cloud.framework.kafka.properties.TopicProvisioningProperties;
import net.hwyz.iov.cloud.framework.kafka.support.KafkaAdminOperations;
import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicProvisioningStatus;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicCatalog;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioner;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningCoordinator;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningMetrics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KafkaTopicProvisioningAutoConfiguration 自动配置测试（ApplicationContextRunner）。
 * 覆盖：默认关闭不装配、启用装配、无 Topic/非法定义 fail-fast、Producer 兼容、无 Micrometer 仍可用。
 */
class KafkaTopicProvisioningAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(KafkaProperties.class, KafkaTopicProvisioningAutoConfigurationTest::kafkaProperties);

    /** 同时装配公共 KafkaAdminOperations 自动配置的 runner，模拟真实应用。 */
    private final ApplicationContextRunner supportRunner = new ApplicationContextRunner()
            .withBean(KafkaProperties.class, KafkaTopicProvisioningAutoConfigurationTest::kafkaProperties)
            .withConfiguration(AutoConfigurations.of(
                    KafkaAdminOperationsAutoConfiguration.class,
                    KafkaTopicProvisioningAutoConfiguration.class));

    private static KafkaProperties kafkaProperties() {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers(List.of("localhost:9094"));
        return properties;
    }

    @Test
    void disabledByDefaultDoesNotCreateProvisioningBeans() {
        runner.withConfiguration(AutoConfigurations.of(KafkaTopicProvisioningAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(Admin.class);
                    assertThat(context).doesNotHaveBean(KafkaTopicCatalog.class);
                    assertThat(context).doesNotHaveBean(KafkaTopicProvisioner.class);
                    assertThat(context).doesNotHaveBean(DefaultKafkaTopicProvisioningStatus.class);
                    assertThat(context).doesNotHaveBean(KafkaTopicProvisioningCoordinator.class);
                });
    }

    @Test
    void enabledWithValidTopicsCreatesProvisioningBeans() {
        runner.withConfiguration(AutoConfigurations.of(KafkaTopicProvisioningAutoConfiguration.class))
                .withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=sample.event",
                        "iov.kafka.topic-provisioning.topics[0].partitions=3",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(Admin.class);
                    assertThat(context).hasSingleBean(KafkaTopicCatalog.class);
                    assertThat(context).hasSingleBean(KafkaTopicProvisioner.class);
                    assertThat(context).hasSingleBean(DefaultKafkaTopicProvisioningStatus.class);
                    assertThat(context).hasSingleBean(KafkaTopicProvisioningCoordinator.class);
                });
    }

    @Test
    void enabledWithoutAnyTopicFailsFast() {
        runner.withConfiguration(AutoConfigurations.of(KafkaTopicProvisioningAutoConfiguration.class))
                .withPropertyValues("iov.kafka.topic-provisioning.enabled=true")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void conflictingDefinitionsFailFast() {
        runner.withConfiguration(AutoConfigurations.of(KafkaTopicProvisioningAutoConfiguration.class))
                .withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=t",
                        "iov.kafka.topic-provisioning.topics[0].partitions=3",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1",
                        "iov.kafka.topic-provisioning.topics[1].name=t",
                        "iov.kafka.topic-provisioning.topics[1].partitions=5",
                        "iov.kafka.topic-provisioning.topics[1].replication-factor=1")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void invalidPartitionsFailFast() {
        runner.withConfiguration(AutoConfigurations.of(KafkaTopicProvisioningAutoConfiguration.class))
                .withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=t",
                        "iov.kafka.topic-provisioning.topics[0].partitions=0",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void existingProducerTemplateBeansArePreserved() {
        runner.withConfiguration(AutoConfigurations.of(
                        KafkaAutoConfiguration.class,
                        KafkaTopicProvisioningAutoConfiguration.class))
                .withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=sample.event",
                        "iov.kafka.topic-provisioning.topics[0].partitions=3",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    Map<String, ReactiveKafkaProducerTemplate<?, ?>> templates =
                            (Map) context.getBeansOfType(ReactiveKafkaProducerTemplate.class);
                    assertThat(templates.keySet()).containsExactlyInAnyOrder(
                            "bytesReactiveKafkaProducerTemplate", "stringReactiveKafkaProducerTemplate");
                    assertThat(context).hasSingleBean(KafkaTopicProvisioningCoordinator.class);
                });
    }

    @Test
    void provisioningWorksWithoutMeterRegistry() {
        runner.withConfiguration(AutoConfigurations.of(KafkaTopicProvisioningAutoConfiguration.class))
                .withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=sample.event",
                        "iov.kafka.topic-provisioning.topics[0].partitions=3",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    // 无 Micrometer 时 Metrics Bean 不装配
                    assertThat(context).doesNotHaveBean(MeterRegistry.class);
                    assertThat(context).doesNotHaveBean(KafkaTopicProvisioningMetrics.class);
                    // 但不影响 Provisioning 组件装配
                    assertThat(context).hasSingleBean(KafkaTopicProvisioningCoordinator.class);
                    assertThat(context).hasSingleBean(KafkaTopicProvisioner.class);
                });
    }

    // ---------- KafkaAdminOperations 公共 Bean（RD-002-6：独立于 enabled 条件） ----------

    @Test
    void adminOperationsAvailableWhenProvisioningDisabled() {
        supportRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(KafkaAdminOperations.class);
            assertThat(context).doesNotHaveBean(Admin.class);
            assertThat(context).doesNotHaveBean(KafkaTopicProvisioningCoordinator.class);
        });
    }

    @Test
    void adminOperationsAvailableWhenProvisioningEnabled() {
        supportRunner.withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=sample.event",
                        "iov.kafka.topic-provisioning.topics[0].partitions=3",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(KafkaAdminOperations.class);
                    assertThat(context).hasSingleBean(KafkaTopicProvisioningCoordinator.class);
                });
    }

    @Test
    void userDefinedKafkaAdminOperationsTakesPrecedence() {
        KafkaAdminOperations custom = new KafkaAdminOperations();
        supportRunner.withBean(KafkaAdminOperations.class, () -> custom)
                .run(context -> {
                    assertThat(context).hasSingleBean(KafkaAdminOperations.class);
                    assertThat(context.getBean(KafkaAdminOperations.class)).isSameAs(custom);
                });
    }

    @Test
    void describeTimeoutAndWarmupDefaultsBindCorrectly() {
        supportRunner.withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=t",
                        "iov.kafka.topic-provisioning.topics[0].partitions=1",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    TopicProvisioningProperties properties = context.getBean(TopicProvisioningProperties.class);
                    assertThat(properties.describeTimeout()).isEqualTo(Duration.ofSeconds(10));
                    assertThat(properties.warmup().enabled()).isTrue();
                    assertThat(properties.warmup().attempts()).isEqualTo(3);
                    assertThat(properties.warmup().initialBackoff()).isEqualTo(Duration.ofSeconds(1));
                    assertThat(properties.retry().initialInterval()).isEqualTo(Duration.ofSeconds(5));
                    assertThat(properties.retry().maxInterval()).isEqualTo(Duration.ofSeconds(60));
                    assertThat(properties.retry().multiplier()).isEqualTo(2.0);
                });
    }

    @Test
    void describeTimeoutAndWarmupExplicitValuesOverrideDefaults() {
        supportRunner.withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.describe-timeout=3s",
                        "iov.kafka.topic-provisioning.warmup.enabled=false",
                        "iov.kafka.topic-provisioning.warmup.attempts=5",
                        "iov.kafka.topic-provisioning.warmup.initial-backoff=500ms",
                        "iov.kafka.topic-provisioning.topics[0].name=t",
                        "iov.kafka.topic-provisioning.topics[0].partitions=1",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    TopicProvisioningProperties properties = context.getBean(TopicProvisioningProperties.class);
                    assertThat(properties.describeTimeout()).isEqualTo(Duration.ofSeconds(3));
                    assertThat(properties.warmup().enabled()).isFalse();
                    assertThat(properties.warmup().attempts()).isEqualTo(5);
                    assertThat(properties.warmup().initialBackoff()).isEqualTo(Duration.ofMillis(500));
                });
    }

    // ---------- Admin 韧性默认参数（RD-002-5：putIfAbsent，显式优先） ----------

    @Test
    void adminDefaultResilienceParamsFilledWhenMissing() {
        Map<String, Object> props = KafkaTopicProvisioningAutoConfiguration.buildAdminProperties(kafkaProperties());

        assertThat(props).containsEntry(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000)
                .containsEntry(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000)
                .containsEntry(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 600_000)
                .containsEntry(AdminClientConfig.METADATA_MAX_AGE_CONFIG, 300_000);
    }

    @Test
    void adminExplicitValuesArePreservedPerKey() {
        Map<String, Object> defaults = Map.of(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000,
                AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 600_000,
                AdminClientConfig.METADATA_MAX_AGE_CONFIG, 300_000);
        for (String key : defaults.keySet()) {
            KafkaProperties properties = kafkaProperties();
            properties.getProperties().put(key, "12345");
            Map<String, Object> props = KafkaTopicProvisioningAutoConfiguration.buildAdminProperties(properties);

            // 显式值保留，不被覆盖
            assertThat(props).containsEntry(key, "12345");
            // 其余三个键仍补齐默认值
            for (Map.Entry<String, Object> other : defaults.entrySet()) {
                if (!other.getKey().equals(key)) {
                    assertThat(props).containsEntry(other.getKey(), other.getValue());
                }
            }
        }
    }
}
