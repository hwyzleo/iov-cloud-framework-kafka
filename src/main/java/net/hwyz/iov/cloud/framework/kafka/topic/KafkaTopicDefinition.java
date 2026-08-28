package net.hwyz.iov.cloud.framework.kafka.topic;

import java.util.Map;

/**
 * Kafka Topic 定义模型。
 *
 * @param name              Topic 名称，不能为空
 * @param partitions        分区数，必须大于等于 1
 * @param replicationFactor 副本数，必须大于等于 1
 * @param configs           Topic 级配置，仅首次创建时使用；已存在 Topic 不执行 alterConfigs
 * @author hwyz_leo
 */
public record KafkaTopicDefinition(
        String name,
        int partitions,
        short replicationFactor,
        Map<String, String> configs) {

    public KafkaTopicDefinition {
        configs = configs == null ? Map.of() : Map.copyOf(configs);
    }

    /**
     * 无 Topic 级配置的便捷构造，使用 broker 默认配置。
     */
    public KafkaTopicDefinition(String name, int partitions, short replicationFactor) {
        this(name, partitions, replicationFactor, Map.of());
    }
}
