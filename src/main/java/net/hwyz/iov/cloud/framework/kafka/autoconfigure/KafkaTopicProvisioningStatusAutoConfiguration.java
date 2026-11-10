package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicProvisioningStatus;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Kafka Topic Provisioning 状态自动配置。
 *
 * <p>状态 Bean 独立于 {@code iov.kafka.topic-provisioning.enabled} 条件：功能关闭时初始状态为
 * {@link KafkaTopicProvisioningStatus.State#DISABLED}，启用后由
 * {@link KafkaTopicProvisioningAutoConfiguration} 切换为
 * {@link KafkaTopicProvisioningStatus.State#NOT_READY}。业务可始终按同一契约注入
 * {@link KafkaTopicProvisioningStatus}，无需关心功能是否启用。</p>
 *
 * <p>用户提供同类型 Bean 时框架自动退让。</p>
 *
 * @author hwyz_leo
 */
@AutoConfiguration
public class KafkaTopicProvisioningStatusAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DefaultKafkaTopicProvisioningStatus kafkaTopicProvisioningStatus() {
        return new DefaultKafkaTopicProvisioningStatus(KafkaTopicProvisioningStatus.State.DISABLED);
    }
}
