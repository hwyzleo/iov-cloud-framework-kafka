package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import io.micrometer.core.instrument.MeterRegistry;
import net.hwyz.iov.cloud.framework.kafka.properties.TopicProvisioningProperties;
import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicCatalog;
import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicProvisioner;
import net.hwyz.iov.cloud.framework.kafka.topic.DefaultKafkaTopicProvisioningStatus;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicCatalog;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicDefinitionProvider;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioner;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningCoordinator;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningMetrics;
import net.hwyz.iov.cloud.framework.kafka.topic.KafkaTopicProvisioningStatus;
import net.hwyz.iov.cloud.framework.kafka.topic.PropertiesKafkaTopicDefinitionProvider;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Kafka Topic Provisioning 自动配置（独立、可选）。
 * <ul>
 *   <li>仅当 {@code iov.kafka.topic-provisioning.enabled=true} 时装配</li>
 *   <li>不修改现有 KafkaAutoConfiguration 的 Producer Template Bean</li>
 *   <li>Admin 连接/认证/SSL/SASL 参数复用 Spring Boot KafkaProperties</li>
 *   <li>MeterRegistry 存在时注册指标，不存在时不影响 Provisioning</li>
 *   <li>不注册 Spring readiness HealthIndicator</li>
 * </ul>
 *
 * <p>装配顺序显式后置到 Spring Boot 的 {@code KafkaAutoConfiguration}：本配置的 Admin Bean
 * 属 Provisioning 专用，不得抢占 Spring Boot 的 {@code kafkaAdmin}（类型
 * {@code org.springframework.kafka.core.KafkaAdmin}）Bean 名，否则在
 * {@code spring.main.allow-bean-definition-overriding=false}（Spring Boot 默认）下必然启动失败。
 * 两个 {@code @ConditionalOnMissingBean} 条件基于不同类型（{@code Admin} 与 {@code KafkaAdmin}），
 * 彼此不会互相退让，只能靠 Bean 名区分。</p>
 *
 * @author hwyz_leo
 */
@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnProperty(prefix = "iov.kafka.topic-provisioning", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(TopicProvisioningProperties.class)
public class KafkaTopicProvisioningAutoConfiguration {

    /**
     * Provisioning 专用 Kafka Admin Bean，应用关闭时释放资源。
     * <p>Bean 名刻意与 Spring Boot 的 {@code kafkaAdmin} 区分；用户提供同类型 {@code Admin} Bean 时框架退让。</p>
     * 在 KafkaProperties.buildAdminProperties() 结果副本上仅补齐未显式配置的韧性默认参数，
     * 用户通过 Spring Boot 配置、环境变量或 Nacos 提供的显式值保持优先。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(Admin.class)
    public Admin kafkaTopicProvisioningAdmin(KafkaProperties properties) {
        return AdminClient.create(buildAdminProperties(properties));
    }

    /**
     * 构建 Admin 连接参数：使用 putIfAbsent 仅补齐缺失的韧性默认值，不修改共享对象。
     */
    static Map<String, Object> buildAdminProperties(KafkaProperties properties) {
        Map<String, Object> adminProperties = new HashMap<>(properties.buildAdminProperties());
        adminProperties.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        adminProperties.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        adminProperties.putIfAbsent(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 600_000);
        adminProperties.putIfAbsent(AdminClientConfig.METADATA_MAX_AGE_CONFIG, 300_000);
        return adminProperties;
    }

    /**
     * 框架内置的配置 Provider。
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaTopicDefinitionProvider propertiesKafkaTopicDefinitionProvider(
            TopicProvisioningProperties properties) {
        return new PropertiesKafkaTopicDefinitionProvider(properties);
    }

    /**
     * 只读 Topic Catalog：合并全部 Provider，构造期 fail-fast 校验。
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaTopicCatalog kafkaTopicCatalog(
            ObjectProvider<KafkaTopicDefinitionProvider> providers) {
        return new DefaultKafkaTopicCatalog(providers.stream().toList());
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaTopicProvisioner kafkaTopicProvisioner(Admin admin,
                                                      TopicProvisioningProperties properties) {
        // describe 整次调用共享 iov.kafka.topic-provisioning.describe-timeout 预算
        return new DefaultKafkaTopicProvisioner(admin, properties.describeTimeout());
    }

    /**
     * 独立 TaskScheduler，避免依赖/占用应用默认调度器。
     *
     * <p>故意<b>不</b>注册为容器 Bean：Spring Boot 的
     * {@code TaskSchedulingAutoConfiguration#taskScheduler} 的条件是
     * {@code @ConditionalOnMissingBean({SchedulingConfigurer, TaskScheduler, ScheduledExecutorService})}，
     * 一旦本配置暴露任意 {@code TaskScheduler} 类型 Bean，就会顶掉应用默认调度器，
     * 使未自定义 {@code SchedulingConfigurer} 的服务的所有 {@code @Scheduled} 任务
     * 落到该单线程池（线程名 {@code kafka-topic-provisioning-*}）上；同时会让业务按类型
     * 注入 {@code TaskScheduler} 时出现多候选。</p>
     */
    private static ThreadPoolTaskScheduler provisioningTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-topic-provisioning-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        // 非容器管理，必须显式初始化线程池
        scheduler.initialize();
        return scheduler;
    }

    @Bean(destroyMethod = "close")
    public KafkaTopicProvisioningCoordinator kafkaTopicProvisioningCoordinator(
            KafkaTopicCatalog catalog,
            KafkaTopicProvisioner provisioner,
            DefaultKafkaTopicProvisioningStatus status,
            ApplicationEventPublisher eventPublisher,
            TopicProvisioningProperties properties,
            ObjectProvider<KafkaTopicProvisioningMetrics> metricsProvider) {
        // 状态 Bean 始终存在（关闭时为 DISABLED），启用 Provisioning 后初始态为 NOT_READY
        status.update(KafkaTopicProvisioningStatus.State.NOT_READY, Set.of(), null, null);
        return new KafkaTopicProvisioningCoordinator(catalog, provisioner, status,
                provisioningTaskScheduler(), eventPublisher, properties, metricsProvider.getIfAvailable());
    }

    /**
     * Micrometer 可用时注册指标；无 MeterRegistry 时跳过但不影响 Provisioning。
     */
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    public KafkaTopicProvisioningMetrics kafkaTopicProvisioningMetrics(
            MeterRegistry meterRegistry,
            DefaultKafkaTopicProvisioningStatus status) {
        return new KafkaTopicProvisioningMetrics(meterRegistry, status);
    }
}
