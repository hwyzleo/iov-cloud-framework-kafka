package net.hwyz.iov.cloud.framework.kafka.support;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 无状态 Kafka Admin 公共操作，供 VMD、MDM、CCS 等服务复用启动期连接预热、有界 describe 与异常解包。
 * <ul>
 *   <li>不创建、不关闭调用方传入的 Admin，不持有其生命周期；线程安全依赖局部变量</li>
 *   <li>describeTopics / describeConfigs 各发起一次批量请求，timeout 是整次方法的共享等待预算</li>
 *   <li>空集合返回不可变空映射且不发起网络调用；空 Admin 或非法参数 fail-fast</li>
 *   <li>warmUp 每次重新执行 describeCluster()，总时长上界约为 attempts × timeout + 退避总和</li>
 * </ul>
 *
 * @author hwyz_leo
 */
public class KafkaAdminOperations {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminOperations.class);

    /**
     * 批量描述 Topic。去重后保留输入顺序，返回不可变映射。
     * 超时消息包含 operation、目标数量、当前目标与 timeout。
     *
     * @throws TimeoutException 整次方法超过 timeout 总预算
     */
    public Map<String, TopicDescription> describeTopics(Admin admin, Collection<String> names, Duration timeout)
            throws TimeoutException {
        validateAdmin(admin);
        validateTimeout(timeout);
        if (names == null || names.isEmpty()) {
            return Map.of();
        }
        Set<String> unique = new LinkedHashSet<>(names);
        for (String name : unique) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Topic 名称不能为空");
            }
        }
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        DescribeTopicsResult result = admin.describeTopics(unique);
        long start = System.nanoTime();
        Map<String, TopicDescription> descriptions = new LinkedHashMap<>();
        for (String name : unique) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw deadlineExceeded("describeTopics", unique.size(), name, timeout);
            }
            try {
                descriptions.put(name, result.topicNameValues().get(name).get(remainingNanos, TimeUnit.NANOSECONDS));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("describeTopics 被中断，target=" + name, ie);
            } catch (ExecutionException ee) {
                rethrow(ee, "describeTopics", unique.size(), name, timeout);
            } catch (TimeoutException te) {
                throw contextualTimeout(timeoutMessage("describeTopics", unique.size(), name, timeout), te);
            }
        }
        log.debug("Kafka describeTopics 完成，targetCount={}，elapsedMs={}",
                unique.size(), (System.nanoTime() - start) / 1_000_000);
        return Collections.unmodifiableMap(descriptions);
    }

    /**
     * 批量描述配置。与 describeTopics 使用同一 deadline 模板：不得因前几个结果快速完成而
     * 为后续结果重新获得完整 timeout。
     *
     * @throws TimeoutException 整次方法超过 timeout 总预算
     */
    public Map<ConfigResource, Config> describeConfigs(Admin admin, Collection<ConfigResource> resources,
                                                       Duration timeout) throws TimeoutException {
        validateAdmin(admin);
        validateTimeout(timeout);
        if (resources == null || resources.isEmpty()) {
            return Map.of();
        }
        Set<ConfigResource> unique = new LinkedHashSet<>(resources);
        for (ConfigResource resource : unique) {
            if (resource == null) {
                throw new IllegalArgumentException("ConfigResource 不能为空");
            }
        }
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        DescribeConfigsResult result = admin.describeConfigs(unique);
        long start = System.nanoTime();
        Map<ConfigResource, Config> configs = new LinkedHashMap<>();
        for (ConfigResource resource : unique) {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw deadlineExceeded("describeConfigs", unique.size(), String.valueOf(resource), timeout);
            }
            try {
                configs.put(resource, result.values().get(resource).get(remainingNanos, TimeUnit.NANOSECONDS));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("describeConfigs 被中断，target=" + resource, ie);
            } catch (ExecutionException ee) {
                rethrow(ee, "describeConfigs", unique.size(), String.valueOf(resource), timeout);
            } catch (TimeoutException te) {
                throw contextualTimeout(
                        timeoutMessage("describeConfigs", unique.size(), String.valueOf(resource), timeout), te);
            }
        }
        log.debug("Kafka describeConfigs 完成，targetCount={}，elapsedMs={}",
                unique.size(), (System.nanoTime() - start) / 1_000_000);
        return Collections.unmodifiableMap(configs);
    }

    /**
     * 通过 describeCluster().clusterId() 预热 Admin 连接，最多尝试 attempts 次。
     * 中间失败按 {@code initialBackoff × 2^(attempt-1)} 指数退避（饱和防溢出），
     * 中间失败仅记录 WARN，恢复成功后记录 INFO；耗尽返回 false，最终 ERROR 留给服务层。
     *
     * @param timeout        每次 Admin 调用的超时上界
     * @param attempts       最大尝试次数，必须大于等于 1
     * @param initialBackoff 首次退避间隔，必须大于等于 0
     */
    public boolean warmUp(Admin admin, Duration timeout, int attempts, Duration initialBackoff) {
        validateAdmin(admin);
        validateTimeout(timeout);
        validateBackoff(initialBackoff);
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts 必须大于等于 1");
        }
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                String clusterId = admin.describeCluster().clusterId().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (attempt == 1) {
                    log.debug("Kafka Admin 预热首次成功，clusterId={}", clusterId);
                } else {
                    log.info("Kafka Admin 预热恢复成功，attempt={}, maxAttempts={}", attempt, attempts);
                }
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("Kafka Admin 预热因线程中断提前结束，attempt={}, maxAttempts={}", attempt, attempts);
                return false;
            } catch (Exception ex) {
                Throwable cause = unwrap(ex);
                if (attempt < attempts) {
                    Duration nextBackoff = backoff(initialBackoff, attempt);
                    log.warn("Kafka Admin 预热尝试失败，attempt={}, maxAttempts={}, exceptionClass={}, nextBackoff={}",
                            attempt, attempts, cause.getClass().getSimpleName(), nextBackoff);
                    if (!sleep(nextBackoff)) {
                        return false;
                    }
                } else {
                    log.warn("Kafka Admin 预热尝试耗尽，attempt={}, maxAttempts={}, exceptionClass={}",
                            attempt, attempts, cause.getClass().getSimpleName());
                    return false;
                }
            }
        }
        return false;
    }

    /** 指数退避：initialBackoff × 2^(attempt-1)，使用饱和算法防止 Duration/long 溢出。 */
    private static Duration backoff(Duration initialBackoff, int attempt) {
        long millis = initialBackoff.toMillis();
        long factor = 1L << Math.min(attempt - 1, 62);
        if (millis > Long.MAX_VALUE / factor) {
            return Duration.ofMillis(Long.MAX_VALUE);
        }
        return Duration.ofMillis(millis * factor);
    }

    private static boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
            return true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** 递归解包 ExecutionException / CompletionException。 */
    private static Throwable unwrap(Throwable ex) {
        Throwable t = ex;
        while ((t instanceof ExecutionException || t instanceof CompletionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    /**
     * 统一异常语义：
     * UnknownTopicOrPartitionException 原样透出；其他 RuntimeException 与 Error 保持类型；
     * TimeoutException 转为带上下文的新 TimeoutException 并保留 cause；其余受检异常包装为 IllegalStateException。
     * 不得把权限、认证、连接或序列化异常误判为 Topic 不存在。
     */
    private static void rethrow(Throwable ex, String operation, int targetCount, String target, Duration timeout)
            throws TimeoutException {
        Throwable cause = unwrap(ex);
        if (cause instanceof UnknownTopicOrPartitionException) {
            throw (UnknownTopicOrPartitionException) cause;
        }
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause instanceof TimeoutException) {
            throw contextualTimeout(timeoutMessage(operation, targetCount, target, timeout), cause);
        }
        throw new IllegalStateException(timeoutMessage(operation, targetCount, target, timeout), cause);
    }

    private static TimeoutException deadlineExceeded(String operation, int targetCount, String target,
                                                     Duration timeout) {
        return new TimeoutException(timeoutMessage(operation, targetCount, target, timeout));
    }

    /** 带上下文的新 TimeoutException；JDK 构造器不支持 cause，使用 initCause 保留。 */
    private static TimeoutException contextualTimeout(String message, Throwable cause) {
        TimeoutException exception = new TimeoutException(message);
        exception.initCause(cause);
        return exception;
    }

    private static String timeoutMessage(String operation, int targetCount, String target, Duration timeout) {
        return operation + " 超时，targetCount=" + targetCount + "，target=" + target + "，timeout=" + timeout;
    }

    private static void validateAdmin(Admin admin) {
        if (admin == null) {
            throw new IllegalArgumentException("Admin 不能为空");
        }
    }

    private static void validateTimeout(Duration timeout) {
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout 必须为正时长");
        }
    }

    private static void validateBackoff(Duration initialBackoff) {
        if (initialBackoff == null || initialBackoff.isNegative()) {
            throw new IllegalArgumentException("initialBackoff 不能为负");
        }
    }
}
