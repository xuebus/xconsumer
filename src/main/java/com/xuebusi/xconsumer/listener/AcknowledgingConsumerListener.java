package com.xuebusi.xconsumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * 消息监听服务器类
 * 单个消息处理
 * 手动提交offset
 * Created by SYJ on 2017/4/1.
 */
public class AcknowledgingConsumerListener implements AcknowledgingMessageListener<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AcknowledgingConsumerListener.class);

    /**
     * 消息监听方法
     *
     * @param record
     * @param acknowledgment
     */
    @Override
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        LOGGER.info("消费消息:{}", record.toString());

        //提交offset
        acknowledgment.acknowledge();
    }
}
