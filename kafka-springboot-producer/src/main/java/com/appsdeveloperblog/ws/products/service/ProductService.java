package com.appsdeveloperblog.ws.products.service;

import com.appsdeveloperblog.ws.products.rest.CreateProductRestModel;
import org.springframework.stereotype.Service;

public interface ProductService {

    String createProductAsync(CreateProductRestModel product);

    String createProductSync(CreateProductRestModel product) throws Exception;
}
