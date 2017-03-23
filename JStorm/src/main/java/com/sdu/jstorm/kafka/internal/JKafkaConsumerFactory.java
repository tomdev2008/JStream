package com.sdu.jstorm.kafka.internal;

import com.sdu.jstorm.kafka.JKafkaSpoutConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author hanhan.zhang
 * */
public interface JKafkaConsumerFactory<K, V> {

    public KafkaConsumer<K, V> createConsumer(JKafkaSpoutConfig<K, V> kafkaSpoutConfig);

}
