package com.sdu.stream.consumer;

import com.google.common.collect.Lists;
import com.sdu.stream.consumer.callback.JConsumeCallback;
import com.sdu.stream.utils.GsonUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public class JKafkaConsumerGroup<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(JKafkaConsumerGroup.class);

    private List<JKafkaConsumerLoop> kafkaConsumerLoops;

    public JKafkaConsumerGroup(Properties props, Collection<String> topics, int partitionSize, ConsumerRebalanceListener balanceListener, JConsumeCallback<K, V> callback) {
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerLoops = Lists.newArrayListWithCapacity(partitionSize);
        for (int i = 0; i < partitionSize; ++i) {
            kafkaConsumerLoops.add(new JKafkaConsumerLoop(props, topics, balanceListener, callback, "kafka-consume-thread-" + i));
        }
    }

    public void start() {
        kafkaConsumerLoops.forEach(Thread::start);
        // JVM退出钩子
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                shutdown();
            }
        });
    }

    public void shutdown() {
        kafkaConsumerLoops.forEach(JKafkaConsumerLoop::shutdown);
    }

    private class JKafkaConsumerLoop extends Thread {

        // KafkaConsumer线程不安全[KafkaConsumer对应TopicPartition]
        private KafkaConsumer<K, V> consumer;

        private JConsumeCallback<K, V> callback;

        private Collection<String> topics;

        private ConsumerRebalanceListener balanceListener;

        JKafkaConsumerLoop(Properties props, Collection<String> topics, ConsumerRebalanceListener balanceListener, JConsumeCallback<K,V> consumeCallback, String threadName) {
            setName(threadName);
            this.callback = consumeCallback;
            this.topics = topics;
            this.balanceListener = balanceListener;
            consumer = new KafkaConsumer<>(props);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(topics, balanceListener);
                while (!Thread.interrupted()) {
                    // 阻塞
                    ConsumerRecords<K, V> consumerRecords = consumer.poll(Long.MAX_VALUE);
                    consumerRecords.forEach(consumerRecord ->
                            callback.onConsume(consumerRecord)
                    );
                }
            } catch (WakeupException e) {
                // ignore
            } finally {
                consumer.close();
            }

        }

        void shutdown() {
            // Note:
            //  Thread全程离开两种方式
            //   1: 对于阻塞操作, 调用Thread.interrupt()会抛出InterruptedException
            //   2: 对于非阻塞操作, 可通过检测线程的中断标志位
            JKafkaConsumerLoop.this.interrupt();
            consumer.wakeup();
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "JK_Message_Group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        int partitionSize = 1;
        List<String> topics = Lists.newArrayList("JK_Message");
        ConsumerRebalanceListener balanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitions.forEach(topicPartition -> {
                    String threadName = Thread.currentThread().getName();
                    LOGGER.info("Thread = {}, partitionRevoked = {}", threadName, GsonUtils.toPrettyJson(topicPartition));
                });
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(topicPartition -> {
                    String threadName = Thread.currentThread().getName();
                    LOGGER.info("Thread = {}, partitionAssigned = {}", threadName, GsonUtils.toPrettyJson(topicPartition));
                });
            }
        };
        JConsumeCallback<String, String> callback = new JConsumeCallback<String, String>() {
            @Override
            public void onConsume(ConsumerRecord<String, String> record) {
                String threadName = Thread.currentThread().getName();
                LOGGER.info("Thread = {}, consume = {}", threadName, GsonUtils.toPrettyJson(record));
            }
        };
        JKafkaConsumerGroup<String, String> consumerGroup = new JKafkaConsumerGroup<>(props, topics, partitionSize, balanceListener, callback);
        consumerGroup.start();
    }

}
