package com.appsdeveloperblog.ws.products.rest;

import com.appsdeveloperblog.ws.products.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping("/products")
public class ProductController {

    @Autowired
    private ProductService productService;

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private static final String LOG_MARKER = "********";

    @PostMapping("/async")
    public ResponseEntity<String> addProduct(@RequestBody CreateProductRestModel product) {

        // productService.createProduct
        // 1. 生成 id
        // 1. 保存 product 到数据库
        // 2. 保存 message 到 topic

        String productId = productService.createProductAsync(product);
        
        if (productId == null) {
            return ResponseEntity
                .status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("创建产品失败");
        }
        
        return ResponseEntity
            .status(HttpStatus.CREATED)
            .body("Product added: " + productId);
        
    }


    @PostMapping("/sync")
    public ResponseEntity<Object> addProductSync(@RequestBody CreateProductRestModel product) {

        try {
            String productId = productService.createProductSync(product);
            return ResponseEntity
                    .status(HttpStatus.CREATED)
                    .body("Product added: " + productId);
        } catch (Exception e) {
//            throw new RuntimeException(e);
            LOGGER.error(LOG_MARKER, e);
            // 使用自定义的 json error : 输出到 client
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorMessage(new Date(), e.getMessage(), "/products"));

        }
    }
}
