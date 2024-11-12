package com.appsdeveloperblog.estore.transfers.service;

import java.net.ConnectException;

import com.appsdeveloperblog.ws.core.error.NotRetryableException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import com.appsdeveloperblog.estore.transfers.error.TransferServiceException;
import com.appsdeveloperblog.estore.transfers.model.TransferRestModel;
import com.appsdeveloperblog.ws.core.events.DepositRequestedEvent;
import com.appsdeveloperblog.ws.core.events.WithdrawalRequestedEvent;

@Service
public class TransferServiceImpl implements TransferService {
	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	private static final String LOG_MARKER = "********";

	private static final String PROCESS = "PROCESSED_";
	private static final String PEND = "PENDING_";


	private KafkaTemplate<String, Object> kafkaTemplate;
	private Environment environment;
	private RestTemplate restTemplate;

	private final RedisTemplate<String, String> redisTemplate;
	private final ObjectMapper objectMapper;

	public TransferServiceImpl(KafkaTemplate<String, Object> kafkaTemplate, Environment environment,
                               RestTemplate restTemplate,
                               RedisTemplate<String, String> redisTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.environment = environment;
		this.restTemplate = restTemplate;
		this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

	@Transactional
	@Override
	public boolean transfer(TransferRestModel transferRestModel) {

		LOGGER.info(LOG_MARKER + "Transfer receive msg: " + transferRestModel);

		// 查询已处理的重复记录
		String processedTransferId = PROCESS + transferRestModel.getTransactionId();

		if(Boolean.TRUE.equals(redisTemplate.hasKey(processedTransferId))){
			LOGGER.error("duplicate transfer id: " + processedTransferId);
			throw new TransferServiceException("Transfer already in progress");
		}

		// 查询 临时状态的 重复记录
		String pendingTransferId = PEND + transferRestModel.getTransactionId();
		if(Boolean.TRUE.equals(redisTemplate.hasKey(pendingTransferId))){
			LOGGER.error("duplicate transfer id: " + pendingTransferId);
			throw new NotRetryableException("Transfer already in progress");
		}


		WithdrawalRequestedEvent withdrawalEvent = new WithdrawalRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());
		DepositRequestedEvent depositEvent = new DepositRequestedEvent(transferRestModel.getSenderId(),
				transferRestModel.getRecepientId(), transferRestModel.getAmount());


		String transferJson;
        try {
            transferJson = objectMapper.writeValueAsString(transferRestModel);
        } catch (JsonProcessingException e) {
			LOGGER.error(LOG_MARKER + "Error in transfer json: " + transferRestModel, e);
            throw new TransferServiceException(e);
        }

		String transactionId = transferRestModel.getTransactionId();

		// 保存到 redis, send to topic
        try {
//			redisTemplate.opsForValue().set(processedTransferId, transferJson);

			// 先保存 为 临时状态
			saveToRedisWithTry(transactionId, transactionId);


			kafkaTemplate.send(environment.getProperty("withdraw-money-topic", "withdraw-money-topic"),
					withdrawalEvent);
			LOGGER.info("Sent event to withdrawal topic.");

			// Business logic that causes and error
			callRemoteServce();

			kafkaTemplate.send(environment.getProperty("deposit-money-topic", "deposit-money-topic"), depositEvent);
			LOGGER.info("Sent event to deposit topic");


			// 如果所有的 ops 都成功, 改变 redis msg 的临时状态
			confirmRedisOps(transactionId, transferJson);

		} catch (Exception ex) {
			LOGGER.error(LOG_MARKER + "Transfer Transaction error: " + ex.getMessage());

			// 发生任何异常, 删除 redis 保存的临时信息
			cancelRedisOps(transactionId);


			throw new TransferServiceException(ex);
		}

		return true;
	}

	private ResponseEntity<String> callRemoteServce() throws Exception {
		String requestUrl = "http://localhost:8082/response/200";
		ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

		if (response.getStatusCode().value() == HttpStatus.SERVICE_UNAVAILABLE.value()) {
			throw new Exception("Destination Microservice not availble");
		}

		if (response.getStatusCode().value() == HttpStatus.OK.value()) {
			LOGGER.info("Received response from mock service: " + response.getBody());
		}
		return response;
	}

	private void saveToRedisWithTry(String id, String modelJson) {
		// 使用特殊标记保存，表示这是临时状态
		redisTemplate.opsForValue().set(PEND + id, modelJson);
		LOGGER.info(LOG_MARKER + "Saved temporary state to redis: " + "PENDING_" + id);
	}

	private void confirmRedisOps(String id, String modelJson){
		// 删除临时状态，保存最终状态
		redisTemplate.delete(PEND + id);
		LOGGER.info(LOG_MARKER + "confirm redis commit");
		LOGGER.info(LOG_MARKER + "delete temporary state to redis: " + "PENDING_" + id);
		redisTemplate.opsForValue().set(PROCESS + id, modelJson);
		LOGGER.info(LOG_MARKER + "confirm redis commit: " + PROCESS + id);
	}

	private void cancelRedisOps(String id){
		// 删除临时状态
		redisTemplate.delete(PEND + id);
		LOGGER.error(LOG_MARKER + "ERROR: delete temporary state to redis: " + id);
	}



}
