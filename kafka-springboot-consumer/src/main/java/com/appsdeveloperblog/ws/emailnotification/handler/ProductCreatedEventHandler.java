package com.appsdeveloperblog.ws.emailnotification.handler;

import com.appsdeveloperblog.ws.core.ProductCreateEvent;
import com.appsdeveloperblog.ws.emailnotification.MessageDeduplicationService;
import com.appsdeveloperblog.ws.emailnotification.error.NotRetryableException;
import com.appsdeveloperblog.ws.emailnotification.error.RetryableException;
import com.appsdeveloperblog.ws.emailnotification.io.ProcessedEventEntity;
import com.appsdeveloperblog.ws.emailnotification.io.ProcessedEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@KafkaListener(topics = "product-created-events-topic",
        groupId = "product-created-events")
public class ProductCreatedEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductCreatedEventHandler.class);
    private static final String LOG_MARKER = "********";

    private RestTemplate restTemplate;


    private final MessageDeduplicationService deduplicationService;

    private static final String PRODUCT_KEY_PREFIX = "product:";
    private static final String PROCESSED_KEY_PREFIX = "msg:processed:";
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;  // 用于JSON序列化


    // constructor 会自动注入在 main 中声明的 bean
    public ProductCreatedEventHandler(RestTemplate restTemplate
    ,  MessageDeduplicationService deduplicationService, RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {

        // then, we can use this resttemplate to send http request to external microservice
        this.restTemplate = restTemplate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;

        this.deduplicationService = deduplicationService;
    }

    @KafkaHandler
    @Transactional
    public void handleCreate(@Payload ProductCreateEvent productCreatedEvent, // @Payload: tell kafka this is The deserialized message content
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, // @Header: Access to message metadata (topic, partition, offset)
                             @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                             @Header(KafkaHeaders.OFFSET) Long offset,
                             @Header(value = "messageId") String messageId, // producerRecord.headers().add("messageId", UUID.randomUUID().toString().getBytes());
                             @Header(KafkaHeaders.RECEIVED_KEY) String messageKey  // productId
                             ) throws JsonProcessingException {

        // 增加 NotRetryable error msg 处理逻辑
//        if (true) throw new NotRetryableException("An error took place. No need to consume this msg again.");

      
        LOGGER.info(LOG_MARKER + "Received message: topic={}, partition={}, offset={}, payload={}",
                topic, partition, offset, productCreatedEvent);

        // check if message was already processed before
        // 检查消息是否已处理
        String processedKey = PROCESSED_KEY_PREFIX + messageId;

        // 检查消息是否已处理
        if (Boolean.TRUE.equals(redisTemplate.hasKey(processedKey))) {
            LOGGER.info(LOG_MARKER+"REDIS: Found a duplicate messageId: {}", messageId);
            // 如果消息存在, 直接抛出 不重试
            throw new NotRetryableException("Message already processed: " + messageId);
        }

        //            // 将产品数据存储到Redis
            String productKey = PRODUCT_KEY_PREFIX + productCreatedEvent.getProductId();
            String productJson = objectMapper.writeValueAsString(productCreatedEvent);
//            // productCreatedEvent -> 转换为 json 字符串

        Boolean result = redisTemplate.execute(new SessionCallback<Boolean>() {
            @Override
            public Boolean execute(RedisOperations operations) throws DataAccessException {
                try {
                    // 开始事务
                    operations.multi();

                    // 存储产品数据
                    operations.opsForValue().set(
                            productKey,
                            productJson,
                            24,
                            TimeUnit.HOURS
                    );

                    // 标记消息已处理
                    operations.opsForValue().set(
                            processedKey,
                            "1",
                            24,
                            TimeUnit.HOURS
                    );

                    // 执行事务并检查结果
                    List<Object> results = operations.exec();
                    return results != null && results.stream()
                            .allMatch(result -> Boolean.TRUE.equals(result));

                } catch (Exception e) {
                    // 如果出错，回滚事务
                    operations.discard();
                    LOGGER.error(LOG_MARKER+ " redis: Transaction failed", e);
                    return false;
                }
            }
        });

        // 如果保存失败, 则可以重试
        if (!Boolean.TRUE.equals(result)) {
            throw new RetryableException("Failed to store data in Redis");
        }


        // 测试 Retryable error
        // 我们收到 ProductCreateEvent 后, 我们需要 send Http req to external microservice
        // 8082 是 mockservice 的端口
        String url = "http://localhost:8082/response/200";
        try {
            ResponseEntity<String> response =
                    restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            // http://localhost:8082/response/200
            if (response.getStatusCode().value() == HttpStatus.OK.value()){
                LOGGER.info(LOG_MARKER + "received response from a remote service: " + response.getBody());

            }
        } catch (ResourceAccessException ex){
            // 如果 external service is not available, we will receive resource access exception
            LOGGER.error(LOG_MARKER + ex.getMessage());
            // 此时我们抛出 retryable
            throw new RetryableException(ex);
        } catch (HttpStatusCodeException ex){
            // http://localhost:8082/response/500
            // 如果遇到 500 code, 说明是 NotRetryableException
            LOGGER.error(LOG_MARKER + ex.getMessage());
            throw new NotRetryableException(ex);
        } catch (Exception ex){
            LOGGER.error(LOG_MARKER + ex.getMessage());
            throw new NotRetryableException(ex);
        }


        // Add your business logic here to handle the product created event
        // For example: send email notification, update database, etc.



    }
}
