package com.sdu.jstorm.translator;

import com.sdu.jstorm.tuple.JStreamTuple;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author hanhan.zhang
 * */
public interface JKafkaTranslator<K, V> extends JTranslator {

    JStreamTuple apply(ConsumerRecord<K, V> record);

}
