package com.appsdeveloperblog.ws.products.service;

import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Service
public class ProductServiceImpl implements ProductService {

    @Autowired
    KafkaTemplate<String, ProductCreateEvent> kafkaTemplate;
    // KafkaTemplate

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_MARKER = "********";

    @Override
    public String createProductAsync(CreateProductRestModel product) {

        String productId = UUID.randomUUID().toString();

        //TODO: persist product details into database table before publishing an event

        ProductCreateEvent productCreateEvent = new ProductCreateEvent();
        productCreateEvent.setProductId(productId);
        productCreateEvent.setPrice(product.getPrice());
        productCreateEvent.setQuantity(product.getQuantity());
        productCreateEvent.setTitle(product.getTitle());


        // publish event with kafka template
        // async publish event
        CompletableFuture<SendResult<String, ProductCreateEvent>> future =
                kafkaTemplate.send("product-created-events-topic", productId, productCreateEvent);

        // 不能直接返回 productId
        // 因为此时是异步发送 message 到 broker
        // 并不能确定 broker 是否成功存储了 msg

        //CompletableFuture 是异步调用的执行结果
        // future.whenComplete() 用于处理结果, 不管是否执行成功
        // 成功, 返回 Result, 执行 thenAccept
        // 失败, 返回 Exception, 执行 exceptionally, 返回 null, 就不会返回 productId
        future
                .thenAccept(result -> {
                    // 消息发送成功的处理
                    ProducerRecord<String, ProductCreateEvent> record = result.getProducerRecord();
                    LOGGER.info("{} Msg sent successfully. {},  Event: {}",
                            LOG_MARKER,
                            result.getRecordMetadata(),
                            record.value()  // ProductCreateEvent
                    );
                    // Msg sent successfully: product-created-events-topic-2@0
                    // 2: 表示分区号（Partition Number）
                    // @0: 表示偏移量（Offset）
                })
                .exceptionally(exception -> {
                    // 消息发送失败的处理
                    LOGGER.error(LOG_MARKER + "Error while sending create product event: " + exception.getMessage());
                    return null;
                });


        return productId;
    }

    @Override
    public String createProductSync(CreateProductRestModel product) throws Exception{
        String productId = UUID.randomUUID().toString();

        //TODO: persist product details into database table before publishing an event

        ProductCreateEvent productCreateEvent = new ProductCreateEvent();
        productCreateEvent.setProductId(productId);
        productCreateEvent.setPrice(product.getPrice());
        productCreateEvent.setQuantity(product.getQuantity());
        productCreateEvent.setTitle(product.getTitle());

        // 方式2：如果必须同步等待，至少加上超时控制

        SendResult<String, ProductCreateEvent> result =
                kafkaTemplate.send("product-created-events-topic", productId, productCreateEvent)
                        .get(10, TimeUnit.SECONDS);

        LOGGER.info("{} Message sent successfully. Topic: {}, Partition: {}, Offset: {}, Event: {}",
                LOG_MARKER,
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset(),
                result.getProducerRecord().value()
        );

        return productId;  // 成功发送后返回 productId



    }
}
