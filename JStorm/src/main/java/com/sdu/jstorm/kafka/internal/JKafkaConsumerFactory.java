package com.sdu.jstorm.kafka.internal;

import com.sdu.jstorm.kafka.JKafkaSpoutConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface JKafkaConsumerFactory<K, V> extends Serializable{

    public KafkaConsumer<K, V> createConsumer(JKafkaSpoutConfig<K, V> kafkaSpoutConfig);

}
