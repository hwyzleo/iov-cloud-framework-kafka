package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicProvisioningStatus;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningCoordinator;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KafkaTopicProvisioningStatusAutoConfiguration 测试：
 * 状态 Bean 独立于 Provisioning 开关，关闭态为 DISABLED，启用后初始态为 NOT_READY。
 */
class KafkaTopicProvisioningStatusAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(KafkaProperties.class, KafkaTopicProvisioningStatusAutoConfigurationTest::kafkaProperties)
            .withConfiguration(AutoConfigurations.of(
                    KafkaTopicProvisioningStatusAutoConfiguration.class,
                    KafkaTopicProvisioningAutoConfiguration.class));

    private static KafkaProperties kafkaProperties() {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers(List.of("localhost:9094"));
        return properties;
    }

    @Test
    void statusBeanIsAvailableWithDisabledStateWhenProvisioningOff() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(KafkaTopicProvisioningStatus.class);
            assertThat(context.getBean(KafkaTopicProvisioningStatus.class).state())
                    .isEqualTo(KafkaTopicProvisioningStatus.State.DISABLED);
            assertThat(context.getBean(KafkaTopicProvisioningStatus.class).missingTopics()).isEmpty();
            assertThat(context.getBean(KafkaTopicProvisioningStatus.class).lastFailure()).isEmpty();
            assertThat(context.getBean(KafkaTopicProvisioningStatus.class).nextRetryAt()).isEmpty();
            assertThat(context).doesNotHaveBean(KafkaTopicProvisioningCoordinator.class);
        });
    }

    @Test
    void statusBeanStartsNotReadyWhenProvisioningOn() {
        runner.withPropertyValues(
                        "iov.kafka.topic-provisioning.enabled=true",
                        "iov.kafka.topic-provisioning.topics[0].name=sample.event",
                        "iov.kafka.topic-provisioning.topics[0].partitions=1",
                        "iov.kafka.topic-provisioning.topics[0].replication-factor=1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(KafkaTopicProvisioningStatus.class);
                    assertThat(context.getBean(KafkaTopicProvisioningStatus.class).state())
                            .isEqualTo(KafkaTopicProvisioningStatus.State.NOT_READY);
                    assertThat(context).hasSingleBean(KafkaTopicProvisioningCoordinator.class);
                });
    }

    @Test
    void userDefinedStatusTakesPrecedence() {
        DefaultKafkaTopicProvisioningStatus custom =
                new DefaultKafkaTopicProvisioningStatus(KafkaTopicProvisioningStatus.State.READY);
        runner.withBean(DefaultKafkaTopicProvisioningStatus.class, () -> custom)
                .run(context -> {
                    assertThat(context).hasSingleBean(KafkaTopicProvisioningStatus.class);
                    assertThat(context.getBean(KafkaTopicProvisioningStatus.class)).isSameAs(custom);
                });
    }
}
