package com.appsdeveloperblog.ws.products;

import com.appsdeveloperblog.ws.core.events.ProductCreateEvent;
import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import com.appsdeveloperblog.ws.products.service.ProductService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

// 自动创建 application context, 将会包括所有的代码组件
// ${spring.embedded.kafka.brokers} 是 placeholder, 它会被实际的 EmbeddedKafka 代替
// @EmbeddedKafka 用于启动 embed kafka server, 不需要连接外部的 kafka server
// 这样数据只存在于 测试环境中, 不会发到 production server
// 可以直接配置 broker 的参数, count: broker number
// controlledShutdown 设置 broker 在关闭的时候, 将 leadership 给到其他的 broker
@DirtiesContext // mark spring application context as dirty, dirty means state has been changed, it will be removed and rebuilt. 可以确保 each test 的运行都有一个 clean state
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // 默认, test 多个 methods, junit 就会创建多个 instance, 但是使用这个 annotation, junit 只会创建 1 个 instance
@ActiveProfiles("test") // specify which profile should be activated when run test: application-test.properties. 会 load 这个配置文件
@EmbeddedKafka(partitions = 3, count = 3, controlledShutdown = true)
@SpringBootTest(properties = "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductServiceIntegrationTest {

    @Autowired
    private ProductService productService;

    @Autowired
    Environment env; // Environment 来自于 application-test.properties

    @Autowired
    private EmbeddedKafkaBroker kafkaBroker;

    private KafkaMessageListenerContainer<String, ProductCreateEvent> container;

    private BlockingQueue<ConsumerRecord<String, ProductCreateEvent>> records;

    @BeforeAll
    void setup() throws InterruptedException {
        // 初始化 kafka Listener
        DefaultKafkaConsumerFactory<String, ProductCreateEvent> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig());

        // 配置 消费的 topic
        ContainerProperties containerProperties = new ContainerProperties(env.getProperty("product-created-events-topic-name"));

        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

        // queue
        records = new LinkedBlockingQueue<>();

        // 配置 container msg listener:  add each received record into queue
        container.setupMessageListener((MessageListener<String, ProductCreateEvent>) records::add);

        container.start();
        // make it wait for kafka partitions to be assigned
        // 一旦 container 启动, 它将一直从特定的 topic 中消费
        // 当有新消息时, msg listener 就会被调用, 将 msg 保存到 queue 中
        ContainerTestUtils.waitForAssignment(container, kafkaBroker.getPartitionsPerTopic());

    }


    @Test
    void testCreateProduct_whenGivenValidProductDetails_successfulSendsKafkaMsg() throws Exception {

        // Arrange
        // 初始化 object, 准备所需的信息
        CreateProductRestModel model = new CreateProductRestModel();
        model.setPrice(BigDecimal.valueOf(12.23));
        model.setQuantity(120);
        model.setTitle("books");

        // Act
        // createProductSync 将会发出 kafka msg
        // to consume the msg, 需要创建一个 Consumer
        productService.createProductSync(model);

        // Assert
        // 读取 record, 当 record 已经存放了 3 秒, 然后remove from queue
        // 如果 3 秒后, record 还是不可用, 则返回 null
        ConsumerRecord<String, ProductCreateEvent> message = records.poll(3, TimeUnit.SECONDS);
        assertNotNull(message);
        assertNotNull(message.key());

        ProductCreateEvent event = message.value();
        assertEquals(model.getTitle(), event.getTitle());
        assertEquals(model.getPrice(), event.getPrice());
        assertEquals(model.getQuantity(), event.getQuantity());

    }

    private Map<String, Object> consumerConfig(){
        Map<String, Object> props = new HashMap<>();
        // 每次 test 运行都会创建新的 embedded kafka brokers, ip 和 port 都会改变
        // 所以不能 hard code broker 的信息
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokersAsString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, env.getProperty("spring.kafka.consumer.group-id"));
        props.put(JsonDeserializer.TRUSTED_PACKAGES, env.getProperty("spring.kafka.consumer.properties.spring.json.trusted.packages"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env.getProperty("spring.kafka.consumer.auto-offset-reset"));
        return props;
    }

    @AfterAll
    void teardown() {
        container.stop();
    }
}
