package com.appsdeveloperblog.ws.core;

import lombok.Data;

import java.math.BigDecimal;

// 当这个 event 被 consumed by kafka topic, what should be done?
// 根据 business needs, 来确定 event 中需要包含什么信息
@Data
public class ProductCreateEvent {

    private String productId;
    private String title;
    private BigDecimal price;
    private Integer quantity;
}
