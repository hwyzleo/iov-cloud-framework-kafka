package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import net.hwyz.iov.cloud.framework.kafka.support.KafkaAdminOperations;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Kafka Admin 公共操作自动配置。
 * <p>独立于 {@code iov.kafka.topic-provisioning.enabled} 条件：仅做 Topic 预检、不启用自动 Provisioning
 * 的 CCS 等服务也能复用 KafkaAdminOperations。用户提供同类型 Bean 时框架自动退让。</p>
 *
 * @author hwyz_leo
 */
@AutoConfiguration
public class KafkaAdminOperationsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public KafkaAdminOperations kafkaAdminOperations() {
        return new KafkaAdminOperations();
    }
}
