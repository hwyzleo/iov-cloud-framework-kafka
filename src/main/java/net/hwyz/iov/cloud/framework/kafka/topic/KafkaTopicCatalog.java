package net.hwyz.iov.cloud.framework.kafka.topic;

import java.util.Collection;
import java.util.Optional;

/**
 * 只读 Kafka Topic Catalog，合并全部 Provider 后形成的不可变快照。
 * <p>供 Topic Provisioning 与业务组件复用同一份 Topic 定义。</p>
 *
 * @author hwyz_leo
 */
public interface KafkaTopicCatalog {

    /**
     * 返回全部 Topic 定义的不可变快照。
     */
    Collection<KafkaTopicDefinition> definitions();

    /**
     * 按名称查找 Topic 定义。
     */
    Optional<KafkaTopicDefinition> find(String topicName);

    /**
     * 判断指定 Topic 是否已声明。
     */
    boolean contains(String topicName);
}
