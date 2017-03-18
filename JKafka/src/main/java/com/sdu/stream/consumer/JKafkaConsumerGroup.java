package com.sdu.stream.consumer;

import com.sdu.stream.consumer.callback.JConsumeCallback;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hanhan.zhang
 * */
public class JKafkaConsumerGroup<K, V> {


    public JKafkaConsumerGroup(Properties props, String group, int partition) {

    }

    private class JKafkaConsumerLoop extends Thread {

        // KafkaConsumer线程不安全[KafkaConsumer对应TopicPartition]
        private KafkaConsumer<K, V> consumer;

        private JConsumeCallback<K, V> callback;

        private AtomicBoolean started = new AtomicBoolean(true);

        JKafkaConsumerLoop(Properties props, Collection<String> topics, ConsumerRebalanceListener balanceListener, JConsumeCallback<K,V> consumeCallback, String threadName) {
            setName(threadName);
            this.callback = consumeCallback;
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(topics, balanceListener);
        }

        @Override
        public void run() {
            while (started.get()) {
                // 阻塞
                ConsumerRecords<K, V> consumerRecords = consumer.poll(Long.MAX_VALUE);
                consumerRecords.forEach(consumerRecord ->
                    callback.onConsume(consumerRecord)
                );
            }
        }

        public void shutdown() {
            started.set(false);
            // Note:
            //  Thread
            JKafkaConsumerLoop.this.interrupt();
        }
    }
}
