package com.xuebusi.xconsumer.config;

import com.xuebusi.xconsumer.listener.AcknowledgingConsumerListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * 消费者配置
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {
    /**
     * kafka服务器地址
     */
    @Value("${kafka.consumer.servers}")
    private String BOOTSTRAP_SERVERS_CONFIG;

    /**
     * 消费者组名
     */
    @Value("${kafka.consumer.group.id}")
    private String GROUP_ID_CONFIG;

    /**
     * 是否自动提交偏移量
     */
    @Value("${kafka.consumer.enable.auto.commit}")
    private String ENABLE_AUTO_COMMIT_CONFIG;

    @Value("${kafka.consumer.session.timeout}")
    private String SESSION_TIMEOUT_MS_CONFIG;

    /**
     * 自动提交偏移量的间隔时间(单位毫秒)
     */
    @Value("${kafka.consumer.auto.commit.interval}")
    private String AUTO_COMMIT_INTERVAL_MS_CONFIG;

    /**
     * 从何处开始消费
     */
    @Value("${kafka.consumer.auto.offset.reset}")
    private String AUTO_OFFSET_RESET_CONFIG;

    /**
     * 要消费的topic
     */
    @Value("${kafka.consumer.topic}")
    private String topic;

    /**
     * 消费者线程数
     */
    @Value("${kafka.consumer.concurrency}")
    private String concurrency;

    /**
     * 消费者配置参数
     * @return
     */
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_CONFIG);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG);
        configProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, AUTO_COMMIT_INTERVAL_MS_CONFIG);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);

        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return configProps;
    }

    /**
     * 消费工厂
     * @return
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public AcknowledgingConsumerListener getMessageListener(){
        return new AcknowledgingConsumerListener();
    }

    /**
     * 消费者容器配置信息
     * @return
     */
    @Bean
    public ContainerProperties getContainerProperties() {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        //offset提交方式:MANUAL表示批量提交,MANUAL_IMMEDIATE表示立即提交
        containerProperties.setAckMode(AbstractMessageListenerContainer.AckMode.MANUAL_IMMEDIATE);
        containerProperties.setMessageListener(getMessageListener());
        return containerProperties;
    }

    /**
     * 多线程消息监听容器
     * @return
     */
    @Bean
    public ConcurrentMessageListenerContainer<String, String> kafkaListenerContainerFactory() {
        ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(consumerFactory(), getContainerProperties());
        container.setConcurrency(Integer.valueOf(concurrency));
        return container;
    }
}