package net.hwyz.iov.cloud.framework.kafka.topic;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 默认只读 Catalog：合并全部 Provider 的 Topic 定义，构造期完成校验并生成不可变快照。
 * <ul>
 *   <li>同名且定义相同 → 去重</li>
 *   <li>同名但 partitions / replicationFactor / configs 冲突 → 启动失败（fail-fast）</li>
 *   <li>名称空、partitions &lt; 1、replicationFactor &lt; 1 → 启动失败（fail-fast）</li>
 *   <li>没有任何 Topic 定义 → 启动失败（fail-fast）</li>
 * </ul>
 *
 * @author hwyz_leo
 */
public class DefaultKafkaTopicCatalog implements KafkaTopicCatalog {

    private final Map<String, KafkaTopicDefinition> definitions;

    public DefaultKafkaTopicCatalog(List<KafkaTopicDefinitionProvider> providers) {
        Map<String, KafkaTopicDefinition> merged = new LinkedHashMap<>();
        for (KafkaTopicDefinitionProvider provider : providers) {
            if (provider == null) {
                continue;
            }
            for (KafkaTopicDefinition definition : provider.topicDefinitions()) {
                if (definition == null) {
                    continue;
                }
                validate(definition);
                KafkaTopicDefinition existing = merged.get(definition.name());
                if (existing == null) {
                    merged.put(definition.name(), definition);
                } else if (!existing.equals(definition)) {
                    throw new IllegalStateException("Topic Provisioning 存在冲突的 Topic 定义: ["
                            + definition.name() + "] 被声明了不同的分区数/副本数/配置");
                }
            }
        }
        if (merged.isEmpty()) {
            throw new IllegalStateException("Topic Provisioning 已启用，但未声明任何 Topic Definition");
        }
        this.definitions = Map.copyOf(merged);
    }

    private void validate(KafkaTopicDefinition definition) {
        if (definition.name() == null || definition.name().isBlank()) {
            throw new IllegalArgumentException("Topic 名称不能为空");
        }
        if (definition.partitions() < 1) {
            throw new IllegalArgumentException("Topic [" + definition.name() + "] 的分区数必须大于等于 1");
        }
        if (definition.replicationFactor() < 1) {
            throw new IllegalArgumentException("Topic [" + definition.name() + "] 的副本数必须大于等于 1");
        }
    }

    @Override
    public java.util.Collection<KafkaTopicDefinition> definitions() {
        return definitions.values();
    }

    @Override
    public Optional<KafkaTopicDefinition> find(String topicName) {
        return Optional.ofNullable(definitions.get(topicName));
    }

    @Override
    public boolean contains(String topicName) {
        return definitions.containsKey(topicName);
    }
}
