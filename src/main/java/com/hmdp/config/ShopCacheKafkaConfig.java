package com.hmdp.config;

import com.hmdp.kafka.message.ShopCacheInvalidationMessage;
import com.hmdp.kafka.SynchronousDeadLetterPublishingRecoverer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@EnableScheduling
@Configuration
public class ShopCacheKafkaConfig {

    @Bean
    public NewTopic shopCacheInvalidationTopic(
            @Value("${hmdp.kafka.shop-cache.topic}") String topic,
            @Value("${hmdp.kafka.shop-cache.partitions:3}") int partitions,
            @Value("${hmdp.kafka.shop-cache.replicas:3}") int replicas,
            @Value("${hmdp.kafka.shop-cache.min-in-sync-replicas:2}") int minInSyncReplicas) {
        return buildTopic(topic, partitions, replicas, minInSyncReplicas);
    }

    @Bean
    public NewTopic shopCacheInvalidationDltTopic(
            @Value("${hmdp.kafka.shop-cache.dlt-topic}") String topic,
            @Value("${hmdp.kafka.shop-cache.partitions:3}") int partitions,
            @Value("${hmdp.kafka.shop-cache.replicas:3}") int replicas,
            @Value("${hmdp.kafka.shop-cache.min-in-sync-replicas:2}") int minInSyncReplicas) {
        return buildTopic(topic, partitions, replicas, minInSyncReplicas);
    }

    @Bean("shopCacheProducerFactory")
    public ProducerFactory<String, ShopCacheInvalidationMessage> shopCacheProducerFactory(
            KafkaProperties kafkaProperties) {
        Map<String, Object> properties = new HashMap<>(kafkaProperties.buildProducerProperties());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean("shopCacheKafkaTemplate")
    public KafkaTemplate<String, ShopCacheInvalidationMessage> shopCacheKafkaTemplate(
            @Qualifier("shopCacheProducerFactory")
            ProducerFactory<String, ShopCacheInvalidationMessage> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean("shopCacheKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, ShopCacheInvalidationMessage>
            shopCacheKafkaListenerContainerFactory(
                    KafkaProperties kafkaProperties,
                    @Qualifier("shopCacheKafkaTemplate")
                    KafkaTemplate<String, ShopCacheInvalidationMessage> kafkaTemplate,
                    @Value("${hmdp.kafka.shop-cache.dlt-topic}") String dltTopic,
                    @Value("${hmdp.kafka.shop-cache.dlt-send-timeout-seconds:10}")
                    long dltSendTimeoutSeconds,
                    @Value("${hmdp.kafka.shop-cache.retry-attempts:3}") long retryAttempts,
                    @Value("${hmdp.kafka.shop-cache.retry-backoff-ms:1000}") long retryBackoffMs,
                    @Value("${hmdp.kafka.shop-cache.consumer-concurrency:1}") int concurrency) {
        Map<String, Object> properties = new HashMap<>(kafkaProperties.buildConsumerProperties());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<ShopCacheInvalidationMessage> valueDeserializer =
                new JsonDeserializer<>(ShopCacheInvalidationMessage.class);
        valueDeserializer.addTrustedPackages("com.hmdp.kafka.message");

        DefaultKafkaConsumerFactory<String, ShopCacheInvalidationMessage> consumerFactory =
                new DefaultKafkaConsumerFactory<>(
                        properties,
                        new StringDeserializer(),
                        valueDeserializer
                );

        SynchronousDeadLetterPublishingRecoverer recoverer =
                new SynchronousDeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(dltTopic, record.partition()),
                dltSendTimeoutSeconds
                );
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(
                recoverer,
                new FixedBackOff(retryBackoffMs, retryAttempts)
        );
        errorHandler.setCommitRecovered(true);

        ConcurrentKafkaListenerContainerFactory<String, ShopCacheInvalidationMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setErrorHandler(errorHandler);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    private NewTopic buildTopic(String topic,
                                int partitions,
                                int replicas,
                                int minInSyncReplicas) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        String.valueOf(minInSyncReplicas))
                .config(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
                .build();
    }
}
