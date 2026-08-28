# iov-cloud-framework-kafka

从零开始练手车联网云端框架 Kafka 部分。

## 能力概述

- **Reactive Producer 自动配置**：自动装配 `ReactiveKafkaProducerTemplate<String, byte[]>` 与 `ReactiveKafkaProducerTemplate<String, String>` 两个 Bean，供业务服务直接注入使用。
- **Kafka Topic Provisioning（可选）**：业务服务声明所需 Topic，框架统一完成 Catalog 合并校验、Topic 存在性检查、幂等创建、指数退避重试、状态传播与可观测性。默认关闭，启用不影响既有 Producer Bean。

## Topic Provisioning 使用方式

### 1. 启用

```yaml
iov:
  kafka:
    topic-provisioning:
      enabled: true
      initial-delay: 0s
      retry:
        initial-interval: 5s
        max-interval: 60s
        multiplier: 2.0
      topics:
        - name: sample.event
          partitions: 3
          replication-factor: 3
          configs:
            cleanup.policy: delete
```

- `enabled` 默认 `false`，未启用时不创建 Admin、Catalog、Coordinator、TaskScheduler 或指标。
- Topic 可由 application 配置或 Nacos 集中配置提供。
- 未设置 `configs` 时使用 broker 默认 Topic 配置。
- Admin 连接、认证、SSL/SASL 参数复用 Spring Boot `spring.kafka.*` 的 Admin 属性。
- `enabled=true` 但无任何 Topic、存在冲突定义或字段非法（名称为空、分区数/副本数小于 1）时启动失败（fail-fast）。

### 2. 代码声明 Topic（可选 SPI）

```java
@Bean
KafkaTopicDefinitionProvider customTopicDefinitionProvider() {
    return () -> List.of(new KafkaTopicDefinition("vehicle.event", 3, (short) 3));
}
```

配置列表与全部 Provider 合并为只读 `KafkaTopicCatalog`；同名且定义相同去重，同名但定义冲突则启动失败。

### 3. 读取状态 / 监听事件

```java
// 轮询
KafkaTopicProvisioningStatus status; // 注入
if (status.state() == KafkaTopicProvisioningStatus.State.READY) { ... }

// 事件驱动
@EventListener
void onReady(KafkaTopicsReadyEvent event) { ... }
```

- 状态包含 `DISABLED / NOT_READY / READY`，实现使用 `AtomicReference` 原子替换不可变快照。
- `KafkaTopicsReadyEvent` 仅在首次由非 READY 进入 READY 时发布一次。

### 4. 可观测性

Micrometer 可用时注册以下指标（无 MeterRegistry 不影响 Provisioning）：

- `iov.kafka.topic.provisioning.ready`（Gauge，READY 为 1）
- `iov.kafka.topic.provisioning.attempt`（Counter）
- `iov.kafka.topic.provisioning.failure`（Counter）
- `iov.kafka.topic.provisioning.created`（Counter）

Provisioning 状态不加入 Spring readiness group。

## 行为边界

- 幂等：已存在 Topic 视为成功；多实例并发创建时 `TopicExistsException` 同样视为成功，不引入分布式锁。
- 重试：broker 故障时服务继续启动并保持 `NOT_READY`，按 `min(initialInterval × multiplier^attempt, maxInterval)` 持续后台重试，broker 恢复后自动切换 `READY`。
- 不内置业务 Topic、不感知业务 Outbox/Relay、不删除或修改已有 Topic、不扩分区、不执行 alterConfigs、不授予 ACL。
- 使用方启用后，运行 Principal 必须具备 `DescribeTopics` / `CreateTopics` 权限。
- 回滚：设置 `iov.kafka.topic-provisioning.enabled=false` 即可停用，不影响现有 Producer。

## 测试

```bash
mvn test
```

- 单元测试：Catalog 合并/去重/冲突 fail-fast、Provisioner 幂等与结构化失败、状态原子切换与 READY 事件、指数退避封顶。
- 自动配置测试：默认关闭不装配、启用装配、无 Topic/非法定义 fail-fast、Producer 兼容、无 Micrometer 仍可用。
- 集成测试：真实 Kafka broker（默认 `localhost:9094`）创建/幂等/配置不修改，以及 Coordinator 端到端装配；broker 不可用时自动跳过。
