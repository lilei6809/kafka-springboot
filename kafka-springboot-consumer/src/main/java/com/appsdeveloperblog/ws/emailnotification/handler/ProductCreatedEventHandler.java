package com.appsdeveloperblog.ws.emailnotification.handler;

import com.appsdeveloperblog.ws.core.ProductCreateEvent;
import com.appsdeveloperblog.ws.emailnotification.error.NotRetryableException;
import com.appsdeveloperblog.ws.emailnotification.error.RetryableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

@Component
@KafkaListener(topics = "product-created-events-topic",
        groupId = "product-created-events")
public class ProductCreatedEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductCreatedEventHandler.class);
    private static final String LOG_MARKER = "********";

    private RestTemplate restTemplate;

    // constructor 会自动注入在 main 中声明的 bean
    public ProductCreatedEventHandler(RestTemplate restTemplate) {

        // then, we can use this resttemplate to send http request to external microservice
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handleCreate(@Payload ProductCreateEvent productCreatedEvent, // @Payload: The deserialized message content
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, // @Header: Access to message metadata (topic, partition, offset)
                             @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                             @Header(KafkaHeaders.OFFSET) Long offset){

        // 增加 NotRetryable error msg 处理逻辑
//        if (true) throw new NotRetryableException("An error took place. No need to consume this msg again.");

        LOGGER.info(LOG_MARKER + "Received message: topic={}, partition={}, offset={}, payload={}",
                topic, partition, offset, productCreatedEvent);

        // 测试 Retryable error
        // 我们收到 ProductCreateEvent 后, 我们需要 send Http req to external microservice
        // 8082 是 mockservice 的端口
        String url = "http://localhost:8082/response/500";
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
