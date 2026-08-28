package net.hwyz.iov.cloud.framework.kafka.topic;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 默认 Topic Provisioner：基于 Kafka Admin 接口实现。
 * <ol>
 *   <li>describeTopics 逐 Topic 查询现有 Topic；UnknownTopicOrPartitionException 判定为缺失</li>
 *   <li>计算缺失集合</li>
 *   <li>批量 createTopics</li>
 *   <li>逐 Topic 汇总结果；TopicExistsException 视为成功（并发/已存在幂等）</li>
 * </ol>
 * describe 阶段出现非"Topic 不存在"异常时，无法可靠判断现状，按设计将全部声明 Topic 记为缺失，
 * 返回结构化失败结果交由 Coordinator 安排重试（不盲目执行创建）。已存在 Topic 不执行 alterConfigs 或扩分区。
 *
 * @author hwyz_leo
 */
public class DefaultKafkaTopicProvisioner implements KafkaTopicProvisioner {

    private final Admin admin;

    public DefaultKafkaTopicProvisioner(Admin admin) {
        this.admin = admin;
    }

    @Override
    public CompletionStage<KafkaTopicProvisioningResult> ensureTopics(
            Collection<KafkaTopicDefinition> definitions) {
        Map<String, KafkaTopicDefinition> byName = new LinkedHashMap<>();
        for (KafkaTopicDefinition definition : definitions) {
            byName.put(definition.name(), definition);
        }
        Set<String> declared = byName.keySet();

        try {
            DescribeTopicsResult describeResult = admin.describeTopics(declared);
            return collectExisting(describeResult, declared)
                    .thenCompose(existing -> {
                        Set<String> missing = new HashSet<>(declared);
                        missing.removeAll(existing);
                        if (missing.isEmpty()) {
                            return CompletableFuture.completedFuture(
                                    KafkaTopicProvisioningResult.success(existing, Set.of()));
                        }
                        return createMissing(missing, byName, existing);
                    })
                    .exceptionally(ex -> toFailureResult(ex, declared));
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    /**
     * 逐 Topic 收集已存在集合；任意非"Topic 不存在"异常视为 describe 不可靠。
     */
    private CompletionStage<Set<String>> collectExisting(DescribeTopicsResult result, Set<String> declared) {
        Set<String> existing = new HashSet<>();
        AtomicReference<Throwable> describeFailure = new AtomicReference<>();
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : result.values().entrySet()) {
            String name = entry.getKey();
            pending.add(toCompletionStage(entry.getValue()).<Void>handle((description, ex) -> {
                if (ex == null) {
                    existing.add(name);
                } else if (!(unwrap(ex) instanceof UnknownTopicOrPartitionException)) {
                    describeFailure.compareAndSet(null, unwrap(ex));
                }
                return null;
            }).toCompletableFuture());
        }
        return CompletableFuture.allOf(pending.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> {
                    Throwable failure = describeFailure.get();
                    if (failure != null) {
                        throw new CompletionException(failure);
                    }
                    return existing;
                });
    }

    private CompletionStage<KafkaTopicProvisioningResult> createMissing(
            Set<String> missing,
            Map<String, KafkaTopicDefinition> byName,
            Set<String> existing) {
        Collection<NewTopic> newTopics = new ArrayList<>();
        for (String name : missing) {
            KafkaTopicDefinition definition = byName.get(name);
            NewTopic newTopic = new NewTopic(definition.name(), definition.partitions(), definition.replicationFactor());
            if (!definition.configs().isEmpty()) {
                newTopic.configs(definition.configs());
            }
            newTopics.add(newTopic);
        }

        CreateTopicsResult result = admin.createTopics(newTopics);
        Set<String> created = new HashSet<>();
        Map<String, Throwable> failures = new HashMap<>();
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
            String name = entry.getKey();
            pending.add(toCompletionStage(entry.getValue()).<Void>handle((ignored, ex) -> {
                if (ex == null) {
                    created.add(name);
                } else if (unwrap(ex) instanceof TopicExistsException) {
                    // 并发/已存在场景下视为成功，不重复创建
                    existing.add(name);
                } else {
                    failures.put(name, unwrap(ex));
                }
                return null;
            }).toCompletableFuture());
        }
        return CompletableFuture.allOf(pending.toArray(new CompletableFuture[0]))
                .thenApply(ignored -> {
                    if (failures.isEmpty()) {
                        return KafkaTopicProvisioningResult.success(existing, created);
                    }
                    Set<String> stillMissing = new HashSet<>(missing);
                    stillMissing.removeAll(created);
                    stillMissing.removeAll(existing);
                    return KafkaTopicProvisioningResult.failure(existing, created, stillMissing, failures);
                });
    }

    private KafkaTopicProvisioningResult toFailureResult(Throwable ex, Set<String> declared) {
        Throwable cause = unwrap(ex);
        Map<String, Throwable> failures = new HashMap<>();
        for (String name : declared) {
            failures.put(name, cause);
        }
        return KafkaTopicProvisioningResult.failure(Set.of(), Set.of(), declared, failures);
    }

    private <T> CompletionStage<T> toCompletionStage(KafkaFuture<T> future) {
        return future.toCompletionStage();
    }

    private Throwable unwrap(Throwable ex) {
        Throwable t = ex;
        while ((t instanceof ExecutionException || t instanceof CompletionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
