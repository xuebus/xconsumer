package com.xuebusi.xconsumer.config;

import com.xuebusi.xconsumer.listener.AcknowledgingConsumerService;
import com.xuebusi.xconsumer.listener.TestListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
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
    @Value("${kafka.consumer.servers}")
    private String BOOTSTRAP_SERVERS_CONFIG;

    @Value("${kafka.consumer.group.id}")
    private String GROUP_ID_CONFIG;

    @Value("${kafka.consumer.enable.auto.commit}")
    private String ENABLE_AUTO_COMMIT_CONFIG;

    @Value("${kafka.consumer.session.timeout}")
    private String SESSION_TIMEOUT_MS_CONFIG;

    @Value("${kafka.consumer.auto.commit.interval}")
    private String AUTO_COMMIT_INTERVAL_MS_CONFIG;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String AUTO_OFFSET_RESET_CONFIG;

    @Value("${kafka.consumer.topic}")
    private String topic;

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

    /*@Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        return factory;
    }*/

    @Bean
    public AcknowledgingConsumerService getMessageListener(){
        return new AcknowledgingConsumerService();
    }

    /*@Bean
    public TestListener listener() {
        return new TestListener();
    }*/


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

    /*@Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(Integer.valueOf(concurrency));
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }*/
}