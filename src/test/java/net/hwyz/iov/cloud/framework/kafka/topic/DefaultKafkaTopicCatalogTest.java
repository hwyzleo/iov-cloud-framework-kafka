package net.hwyz.iov.cloud.framework.kafka.topic;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * DefaultKafkaTopicCatalog 单元测试：合并、去重、冲突与非法定义 fail-fast。
 */
class DefaultKafkaTopicCatalogTest {

    private static KafkaTopicDefinition def(String name, int partitions, short replication) {
        return new KafkaTopicDefinition(name, partitions, replication);
    }

    @Test
    void mergesDefinitionsFromMultipleProviders() {
        KafkaTopicDefinitionProvider providerA = () -> List.of(def("a", 3, (short) 1));
        KafkaTopicDefinitionProvider providerB = () -> List.of(def("b", 5, (short) 2));

        DefaultKafkaTopicCatalog catalog = new DefaultKafkaTopicCatalog(List.of(providerA, providerB));

        assertThat(catalog.definitions()).hasSize(2);
        assertThat(catalog.contains("a")).isTrue();
        assertThat(catalog.find("b")).contains(def("b", 5, (short) 2));
        assertThat(catalog.find("unknown")).isEqualTo(Optional.empty());
    }

    @Test
    void deduplicatesIdenticalDefinitions() {
        KafkaTopicDefinitionProvider providerA = () -> List.of(def("a", 3, (short) 1));
        KafkaTopicDefinitionProvider providerB = () -> List.of(def("a", 3, (short) 1));

        DefaultKafkaTopicCatalog catalog = new DefaultKafkaTopicCatalog(List.of(providerA, providerB));

        assertThat(catalog.definitions()).hasSize(1);
    }

    @Test
    void conflictingDefinitionsFailFast() {
        KafkaTopicDefinitionProvider providerA = () -> List.of(def("a", 3, (short) 1));
        KafkaTopicDefinitionProvider providerB = () -> List.of(def("a", 5, (short) 1));

        assertThatThrownBy(() -> new DefaultKafkaTopicCatalog(List.of(providerA, providerB)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("冲突");
    }

    @Test
    void conflictingConfigsFailFast() {
        KafkaTopicDefinitionProvider providerA = () -> List.of(
                new KafkaTopicDefinition("a", 3, (short) 1, Map.of("cleanup.policy", "delete")));
        KafkaTopicDefinitionProvider providerB = () -> List.of(
                new KafkaTopicDefinition("a", 3, (short) 1, Map.of("cleanup.policy", "compact")));

        assertThatThrownBy(() -> new DefaultKafkaTopicCatalog(List.of(providerA, providerB)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void blankTopicNameFailsFast() {
        assertThatThrownBy(() -> new DefaultKafkaTopicCatalog(
                List.of(() -> List.of(new KafkaTopicDefinition("  ", 3, (short) 1)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("名称不能为空");
    }

    @Test
    void invalidPartitionsFailsFast() {
        assertThatThrownBy(() -> new DefaultKafkaTopicCatalog(
                List.of(() -> List.of(new KafkaTopicDefinition("a", 0, (short) 1)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("分区数");
    }

    @Test
    void invalidReplicationFactorFailsFast() {
        assertThatThrownBy(() -> new DefaultKafkaTopicCatalog(
                List.of(() -> List.of(new KafkaTopicDefinition("a", 3, (short) 0)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("副本数");
    }

    @Test
    void emptyCatalogFailsFast() {
        assertThatThrownBy(() -> new DefaultKafkaTopicCatalog(List.of(() -> List.of())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("未声明任何 Topic");
    }

    @Test
    void snapshotIsImmutable() {
        DefaultKafkaTopicCatalog catalog = new DefaultKafkaTopicCatalog(
                List.of(() -> List.of(def("a", 3, (short) 1))));
        assertThatThrownBy(() -> catalog.definitions().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
