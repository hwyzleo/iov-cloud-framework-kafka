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
import net.hwyz.iov.cloud.framework.kafka.topic.PropertiesKafkaTopicDefinitionProvider;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.HashMap;
import java.util.Map;

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
 * @author hwyz_leo
 */
@AutoConfiguration
@ConditionalOnProperty(prefix = "iov.kafka.topic-provisioning", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(TopicProvisioningProperties.class)
public class KafkaTopicProvisioningAutoConfiguration {

    /**
     * Kafka Admin Bean，应用关闭时释放资源。
     * 在 KafkaProperties.buildAdminProperties() 结果副本上仅补齐未显式配置的韧性默认参数，
     * 用户通过 Spring Boot 配置、环境变量或 Nacos 提供的显式值保持优先。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(Admin.class)
    public Admin kafkaAdmin(KafkaProperties properties) {
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
    public KafkaTopicProvisioner kafkaTopicProvisioner(Admin admin) {
        return new DefaultKafkaTopicProvisioner(admin);
    }

    @Bean
    public DefaultKafkaTopicProvisioningStatus kafkaTopicProvisioningStatus() {
        return new DefaultKafkaTopicProvisioningStatus();
    }

    /**
     * 独立 TaskScheduler，避免依赖/占用应用默认调度器。
     */
    @Bean
    public TaskScheduler kafkaTopicProvisioningTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-topic-provisioning-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        return scheduler;
    }

    @Bean
    public KafkaTopicProvisioningCoordinator kafkaTopicProvisioningCoordinator(
            KafkaTopicCatalog catalog,
            KafkaTopicProvisioner provisioner,
            DefaultKafkaTopicProvisioningStatus status,
            TaskScheduler taskScheduler,
            ApplicationEventPublisher eventPublisher,
            TopicProvisioningProperties properties,
            ObjectProvider<KafkaTopicProvisioningMetrics> metricsProvider) {
        return new KafkaTopicProvisioningCoordinator(catalog, provisioner, status, taskScheduler,
                eventPublisher, properties, metricsProvider.getIfAvailable());
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
