package com.appsdeveloperblog.ws.emailnotification.handler;

import jdk.jfr.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "product-created-events-topic",
        groupId = "product-created-events")
public class ProductCreatedEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProductCreatedEventHandler.class);

    
    @KafkaHandler
    public void handleCreate(@Payload ProductCreatedEvent productCreatedEvent, // @Payload: The deserialized message content
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, // @Header: Access to message metadata (topic, partition, offset)
                       @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                       @Header(KafkaHeaders.OFFSET) Long offset){

        LOGGER.info("Received message: topic={}, partition={}, offset={}, payload={}",
                topic, partition, offset, productCreatedEvent);

        // Add your business logic here to handle the product created event
        // For example: send email notification, update database, etc.

    }
}
