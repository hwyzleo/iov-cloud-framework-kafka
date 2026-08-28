package net.hwyz.iov.cloud.framework.kafka.properties;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Topic 定义的配置绑定 DTO（可变 JavaBean，用于 {@code iov.kafka.topic-provisioning.topics} 属性绑定）。
 * <p>Spring Boot 3.0 对集合内嵌套 record 绑定存在限制，故采用本 DTO 绑定后由
 * {@code PropertiesKafkaTopicDefinitionProvider} 转换为不可变 {@code KafkaTopicDefinition} 记录。</p>
 *
 * @author hwyz_leo
 */
public class KafkaTopicDefinitionProperties {

    private String name;
    private int partitions;
    private short replicationFactor;
    private Map<String, String> configs = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, String> configs) {
        this.configs = configs;
    }
}
