package com.sdu.jstorm.kafka.internal;

import com.sdu.jstorm.kafka.JKafkaSpoutConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class JDefaultKafkaConsumerFactory<K, V> implements JKafkaConsumerFactory<K, V> {

    @Override
    public KafkaConsumer<K, V> createConsumer(JKafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        Map<String, Object> kafkaProps = kafkaSpoutConfig.getKafkaProps();
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaSpoutConfig.getConsumerGroupId());
        return new KafkaConsumer<>(kafkaProps);
    }
}
