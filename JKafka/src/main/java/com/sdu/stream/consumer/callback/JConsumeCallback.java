package com.sdu.stream.consumer.callback;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author hanhan.zhang
 * */
public interface JConsumeCallback<K, V> {

    public void onConsume(ConsumerRecord<K,V> record);

}
