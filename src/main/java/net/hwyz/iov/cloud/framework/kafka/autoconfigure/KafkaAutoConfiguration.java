package net.hwyz.iov.cloud.framework.kafka.autoconfigure;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

/**
 * 自动配置类
 *
 * @author hwyz_leo
 */
@Configuration
@RequiredArgsConstructor
public class KafkaAutoConfiguration {

    private final KafkaProperties properties;

    @Bean
    public ReactiveKafkaProducerTemplate<String, byte[]> bytesReactiveKafkaProducerTemplate() {
        Map<String, Object> props = properties.buildProducerProperties();
        props.put("key.serializer", StringSerializer.class);
        return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(props));
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, String> stringReactiveKafkaProducerTemplate() {
        Map<String, Object> props = properties.buildProducerProperties();
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        return new ReactiveKafkaProducerTemplate<>(SenderOptions.create(props));
    }

}
