package com.xuebusi.xconsumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.Optional;

/**
 * 测试消息消费
 * Created by SYJ on 2017/11/23.
 */
public class TestListener {

    //@KafkaListener(topics = {"app_log"})
    public void listen(ConsumerRecord<?, ?> record) {
        Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        Optional<Long> offset = Optional.ofNullable(record.offset());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            System.out.println("消费消息-偏移量[" + offset + "],消息:[" + message + "]");
        }
    }
}
