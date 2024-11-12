package com.appsdeveloperblog.ws.core.error;

public class RetryableException extends RuntimeException{

    // 接受 custom error message
    public RetryableException(String message) {
        super(message);
    }

    // 接受 original exception that cause the error, then pass this exception to the parent class
    public RetryableException(Throwable cause) {
        super(cause);
    }
}
