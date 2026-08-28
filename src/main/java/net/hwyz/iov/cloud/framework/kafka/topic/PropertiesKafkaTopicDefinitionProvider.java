package net.hwyz.iov.cloud.framework.kafka.topic;

import net.hwyz.iov.cloud.framework.kafka.properties.KafkaTopicDefinitionProperties;
import net.hwyz.iov.cloud.framework.kafka.properties.TopicProvisioningProperties;

import java.util.Collection;
import java.util.List;

/**
 * 框架内置 Provider：从 {@code iov.kafka.topic-provisioning.topics} 配置读取 Topic 定义。
 * <p>配置可来自 application 配置或 Nacos 集中配置；绑定 DTO 在此转换为不可变记录。</p>
 *
 * @author hwyz_leo
 */
public class PropertiesKafkaTopicDefinitionProvider implements KafkaTopicDefinitionProvider {

    private final List<KafkaTopicDefinition> topics;

    public PropertiesKafkaTopicDefinitionProvider(TopicProvisioningProperties properties) {
        this.topics = properties.topics().stream()
                .map(PropertiesKafkaTopicDefinitionProvider::toDefinition)
                .toList();
    }

    private static KafkaTopicDefinition toDefinition(KafkaTopicDefinitionProperties dto) {
        return new KafkaTopicDefinition(
                dto.getName(), dto.getPartitions(), dto.getReplicationFactor(), dto.getConfigs());
    }

    @Override
    public Collection<KafkaTopicDefinition> topicDefinitions() {
        return topics;
    }
}
