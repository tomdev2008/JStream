package com.sdu.jstorm.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.tuple.Fields;

import java.util.Collections;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public interface JTranslator<K, V> {

    public static final List<String> DEFAULT_STREAM = Collections.singletonList("default");

    JKafkaTuple apply(ConsumerRecord<K, V> record);

    Fields getFieldsFor(String stream);

    default List<String> streams() {
        return DEFAULT_STREAM;
    }

}
