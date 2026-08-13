package com.hmdp.config;

import com.hmdp.kafka.SynchronousDeadLetterPublishingRecoverer;
import com.hmdp.kafka.message.SeckillVoucherLocalCacheInvalidationMessage;
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
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class SeckillVoucherCacheKafkaConfig {

    @Bean
    public NewTopic seckillVoucherLocalCacheInvalidationTopic(
            @Value("${hmdp.kafka.seckill-voucher-cache.topic}") String topic,
            @Value("${hmdp.kafka.seckill-voucher-cache.partitions:3}") int partitions,
            @Value("${hmdp.kafka.seckill-voucher-cache.replicas:3}") int replicas,
            @Value("${hmdp.kafka.seckill-voucher-cache.min-in-sync-replicas:2}")
            int minInSyncReplicas) {
        return buildTopic(topic, partitions, replicas, minInSyncReplicas);
    }

    @Bean
    public NewTopic seckillVoucherLocalCacheInvalidationDltTopic(
            @Value("${hmdp.kafka.seckill-voucher-cache.dlt-topic}") String topic,
            @Value("${hmdp.kafka.seckill-voucher-cache.partitions:3}") int partitions,
            @Value("${hmdp.kafka.seckill-voucher-cache.replicas:3}") int replicas,
            @Value("${hmdp.kafka.seckill-voucher-cache.min-in-sync-replicas:2}")
            int minInSyncReplicas) {
        return buildTopic(topic, partitions, replicas, minInSyncReplicas);
    }

    @Bean("seckillVoucherCacheProducerFactory")
    public ProducerFactory<String, SeckillVoucherLocalCacheInvalidationMessage>
            seckillVoucherCacheProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> properties =
                new HashMap<>(kafkaProperties.buildProducerProperties());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean("seckillVoucherCacheKafkaTemplate")
    public KafkaTemplate<String, SeckillVoucherLocalCacheInvalidationMessage>
            seckillVoucherCacheKafkaTemplate(
                    @Qualifier("seckillVoucherCacheProducerFactory")
                    ProducerFactory<String, SeckillVoucherLocalCacheInvalidationMessage>
                            producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean("seckillVoucherCacheKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<
            String, SeckillVoucherLocalCacheInvalidationMessage>
            seckillVoucherCacheKafkaListenerContainerFactory(
                    KafkaProperties kafkaProperties,
                    @Qualifier("seckillVoucherCacheKafkaTemplate")
                    KafkaTemplate<String, SeckillVoucherLocalCacheInvalidationMessage>
                            kafkaTemplate,
                    @Value("${hmdp.kafka.seckill-voucher-cache.dlt-topic}") String dltTopic,
                    @Value("${hmdp.kafka.seckill-voucher-cache.dlt-send-timeout-seconds:10}")
                    long dltSendTimeoutSeconds,
                    @Value("${hmdp.kafka.seckill-voucher-cache.retry-attempts:3}")
                    long retryAttempts,
                    @Value("${hmdp.kafka.seckill-voucher-cache.retry-backoff-ms:1000}")
                    long retryBackoffMs,
                    @Value("${hmdp.kafka.seckill-voucher-cache.consumer-concurrency:1}")
                    int concurrency) {
        Map<String, Object> properties =
                new HashMap<>(kafkaProperties.buildConsumerProperties());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        JsonDeserializer<SeckillVoucherLocalCacheInvalidationMessage> valueDeserializer =
                new JsonDeserializer<>(SeckillVoucherLocalCacheInvalidationMessage.class);
        valueDeserializer.addTrustedPackages("com.hmdp.kafka.message");
        DefaultKafkaConsumerFactory<String, SeckillVoucherLocalCacheInvalidationMessage>
                consumerFactory = new DefaultKafkaConsumerFactory<>(
                        properties, new StringDeserializer(), valueDeserializer);

        SynchronousDeadLetterPublishingRecoverer recoverer =
                new SynchronousDeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) -> new TopicPartition(dltTopic, record.partition()),
                        dltSendTimeoutSeconds);
        SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(
                recoverer, new FixedBackOff(retryBackoffMs, retryAttempts));
        errorHandler.setCommitRecovered(true);

        ConcurrentKafkaListenerContainerFactory<
                String, SeckillVoucherLocalCacheInvalidationMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setErrorHandler(errorHandler);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    private NewTopic buildTopic(
            String topic, int partitions, int replicas, int minInSyncReplicas) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        String.valueOf(minInSyncReplicas))
                .config(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
                .build();
    }
}
