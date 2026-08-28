package net.hwyz.iov.cloud.framework.kafka.topic;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * Kafka Topic Provisioner：检查并创建缺失 Topic。
 * <p>幂等：已存在 Topic 视为成功；多实例并发创建同一 Topic 时，TopicExists 同样视为成功。</p>
 *
 * @author hwyz_leo
 */
@FunctionalInterface
public interface KafkaTopicProvisioner {

    /**
     * 确保声明的 Topic 全部存在，返回结构化结果。
     *
     * @param definitions 待保障的 Topic 定义集合
     */
    CompletionStage<KafkaTopicProvisioningResult> ensureTopics(
            Collection<KafkaTopicDefinition> definitions);
}
