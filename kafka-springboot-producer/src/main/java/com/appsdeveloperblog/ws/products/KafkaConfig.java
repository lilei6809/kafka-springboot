package com.appsdeveloperblog.ws.products;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

// 用于创建 kafka related beans, 所有使用 @Configuration
@Configuration
public class KafkaConfig {

    // 使用 @Value 注解从 application.properties 文件中获取参数值
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.producer.acks}")
    private String acks;

    @Value("${spring.kafka.producer.retries}")
    private int retries;

    @Value("${spring.kafka.producer.retry-backoff-ms}")
    private int retryBackoffMs;

    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;

    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;

    @Value("${spring.kafka.producer.properties.enable.idempotance}")
    private String idempotance;

    @Value("${spring.kafka.producer.properties.max.in.flight.requests.per.connection}")
    private String inFlightRequests;


    Map<String, Object> producerConfigs() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.RETRIES_CONFIG, retries);
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotance);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, inFlightRequests);
//        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return config;
    }

    @Bean
    ProducerFactory<String, ProductCreateEvent> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    KafkaTemplate<String, ProductCreateEvent> kafkaTemplate(){
        return new KafkaTemplate<String, ProductCreateEvent>(producerFactory());
    }

    // 创建 topic 作为 bean 对象
    @Bean
    NewTopic createTopic(){
        // 创建一个名为 product-created-events-topic 的 topic, 并设置分区数为 3, 副本数为 3
        return TopicBuilder
                .name("product-created-events-topic")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min.insync.replicas", "2"))
                .build();
    }


    
}
