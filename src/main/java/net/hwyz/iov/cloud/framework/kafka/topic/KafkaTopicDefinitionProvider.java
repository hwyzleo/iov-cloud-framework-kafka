package net.hwyz.iov.cloud.framework.kafka.topic;

import java.util.Collection;

/**
 * Kafka Topic Definition 提供方 SPI。
 * <p>框架内置 Properties Provider；业务服务可额外声明 Provider Bean，
 * 全部 Provider 的定义由 {@link KafkaTopicCatalog} 合并为同一 SSOT。</p>
 *
 * @author hwyz_leo
 */
@FunctionalInterface
public interface KafkaTopicDefinitionProvider {

    /**
     * 返回本提供方声明的 Topic 定义集合。
     */
    Collection<KafkaTopicDefinition> topicDefinitions();
}
