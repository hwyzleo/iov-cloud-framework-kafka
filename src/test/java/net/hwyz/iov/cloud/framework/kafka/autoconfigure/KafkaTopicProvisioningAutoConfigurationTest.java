package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicProvisioningStatus;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicCatalog;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioner;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningCoordinator;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningMetrics;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;

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
}
