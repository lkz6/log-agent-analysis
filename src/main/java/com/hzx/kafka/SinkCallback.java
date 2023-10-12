package com.hzx.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkCallback implements Callback {
    private static final Logger logger = LoggerFactory.getLogger(SinkCallback.class);
    private long startTime;

    public SinkCallback(long startTime) {
        this.startTime = startTime;
    }

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            logger.warn("Error sending message to Kafka {} ", exception.getMessage());
        }

        if (logger.isDebugEnabled()) {
            long eventElapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                logger.debug("Acked message partition:{} ofset:{}", metadata.partition(),
                        metadata.offset());
            }
            logger.debug("Elapsed time for send: {}", eventElapsedTime);
        }
    }
}
