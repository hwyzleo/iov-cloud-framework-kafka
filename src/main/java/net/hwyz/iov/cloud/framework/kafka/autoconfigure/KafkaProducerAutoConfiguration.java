package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

/**
 * Kafka 生产器自动配置（仅提供两个 Reactive Producer Template）。
 *
 * <p>类名刻意不叫 {@code KafkaAutoConfiguration}：Spring Boot 自身也有
 * {@code org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration}，同名会让
 * {@code AutoConfigurations.of(KafkaAutoConfiguration.class)}、{@code @AutoConfigureAfter} 等
 * 引用在同包/同工程内静默指向错误的类，掩盖真实装配冲突。本类只负责生产器，Bean 名称与
 * 泛型保持历史契约不变。</p>
 *
 * <p>{@code stringReactiveKafkaProducerTemplate} 为 String/String；
 * {@code bytesReactiveKafkaProducerTemplate} 为 String/byte[]，用于原样生产序列化 Proto bytes
 * （如 VehicleMessageEnvelope、GatewayDeliveryStatus）。注意：Spring Boot 的
 * {@link KafkaProperties} 默认 value.serializer 为 StringSerializer，因此 byte[] 生产器
 * 必须显式指定 {@link ByteArraySerializer}，否则发送时抛
 * {@code ClassCastException: [B cannot be cast to String}。
 *
 * @author hwyz_leo
 */
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaProducerAutoConfiguration {

    private final KafkaProperties properties;

    @Bean
    public ReactiveKafkaProducerTemplate<String, byte[]> bytesReactiveKafkaProducerTemplate() {
        return new ReactiveKafkaProducerTemplate<>(
                SenderOptions.create(bytesProducerProperties(properties)));
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, String> stringReactiveKafkaProducerTemplate() {
        return new ReactiveKafkaProducerTemplate<>(
                SenderOptions.create(stringProducerProperties(properties)));
    }

    /**
     * byte[] value 生产器配置（Key=String，Value=raw bytes，可单测）。
     */
    static Map<String, Object> bytesProducerProperties(KafkaProperties properties) {
        Map<String, Object> props = properties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return props;
    }

    /**
     * String value 生产器配置（Key=String，Value=String，可单测）。
     */
    static Map<String, Object> stringProducerProperties(KafkaProperties properties) {
        Map<String, Object> props = properties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

}
