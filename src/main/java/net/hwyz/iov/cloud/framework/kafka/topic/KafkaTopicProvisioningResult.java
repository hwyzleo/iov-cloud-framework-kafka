package net.hwyz.iov.cloud.framework.kafka.topic;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * 单轮 Topic Provisioning 的结构化结果。
 *
 * @param successful     本轮是否全部 Topic 已存在或创建成功
 * @param existingTopics 本轮已存在的 Topic
 * @param createdTopics  本轮由本实例创建成功的 Topic
 * @param missingTopics  本轮仍缺失（创建失败）的 Topic
 * @param failures       按 Topic 归类的失败原因，key 为 Topic 名称
 * @author hwyz_leo
 */
public record KafkaTopicProvisioningResult(
        boolean successful,
        Set<String> existingTopics,
        Set<String> createdTopics,
        Set<String> missingTopics,
        Map<String, Throwable> failures) {

    public KafkaTopicProvisioningResult {
        existingTopics = Collections.unmodifiableSet(Set.copyOf(existingTopics));
        createdTopics = Collections.unmodifiableSet(Set.copyOf(createdTopics));
        missingTopics = Collections.unmodifiableSet(Set.copyOf(missingTopics));
        failures = Collections.unmodifiableMap(Map.copyOf(failures));
    }

    /**
     * 全部成功的结果。
     */
    public static KafkaTopicProvisioningResult success(Set<String> existingTopics, Set<String> createdTopics) {
        return new KafkaTopicProvisioningResult(true, existingTopics, createdTopics,
                Collections.emptySet(), Collections.emptyMap());
    }

    /**
     * 存在失败的结果。
     */
    public static KafkaTopicProvisioningResult failure(Set<String> existingTopics, Set<String> createdTopics,
                                                       Set<String> missingTopics, Map<String, Throwable> failures) {
        return new KafkaTopicProvisioningResult(false, existingTopics, createdTopics, missingTopics, failures);
    }
}
