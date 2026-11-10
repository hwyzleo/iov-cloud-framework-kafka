package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KafkaProducerAutoConfiguration 生产器序列化器契约测试。
 *
 * <p>Spring Boot 的 KafkaProperties 默认 value.serializer 为 StringSerializer；
 * byte[] 生产器必须显式覆盖为 ByteArraySerializer，否则 raw Proto bytes 生产会在
 * 发送阶段抛 {@code ClassCastException: [B cannot be cast to String}。
 */
class KafkaProducerAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(KafkaProperties.class, KafkaProperties::new);

    @Test
    void bytesProducerUsesByteArrayValueSerializer() {
        Map<String, Object> props = KafkaProducerAutoConfiguration.bytesProducerProperties(new KafkaProperties());

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(ByteArraySerializer.class);
    }

    @Test
    void stringProducerUsesStringSerializers() {
        Map<String, Object> props = KafkaProducerAutoConfiguration.stringProducerProperties(new KafkaProperties());

        assertThat(props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringSerializer.class);
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringSerializer.class);
    }

    @Test
    void bothProducerTemplatesAreRegistered() {
        runner.withConfiguration(AutoConfigurations.of(KafkaProducerAutoConfiguration.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    Map<String, ReactiveKafkaProducerTemplate<?, ?>> templates =
                            (Map) context.getBeansOfType(ReactiveKafkaProducerTemplate.class);
                    assertThat(templates.keySet()).containsExactlyInAnyOrder(
                            "bytesReactiveKafkaProducerTemplate", "stringReactiveKafkaProducerTemplate");
                });
    }
}
