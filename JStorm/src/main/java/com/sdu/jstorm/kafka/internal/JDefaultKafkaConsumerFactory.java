package com.sdu.jstorm.kafka.internal;

import com.sdu.jstorm.kafka.JKafkaSpoutConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author hanhan.zhang
 * */
public class JDefaultKafkaConsumerFactory<K, V> implements JKafkaConsumerFactory<K, V> {

    @Override
    public KafkaConsumer<K, V> createConsumer(JKafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        return new KafkaConsumer<>(kafkaSpoutConfig.getKafkaProps(), kafkaSpoutConfig.getKeyDeserializer(), kafkaSpoutConfig.getValueDeserializer());
    }
}
