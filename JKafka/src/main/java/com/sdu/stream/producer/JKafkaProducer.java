package com.sdu.stream.producer;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.sdu.stream.bean.KafkaMessage;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author hanhan.zhang
 * */
public class JKafkaProducer<K,V> {

    // KafkaProducer线程安全
    private KafkaProducer<K, V> producer;

    public JKafkaProducer(Properties config) {
        producer = new KafkaProducer<>(config);
    }

    public List<PartitionInfo> partitionsByTopic(String topic) {
        if (Strings.isNullOrEmpty(topic)) {
            return Collections.emptyList();
        }
        return producer.partitionsFor(topic);
    }

    public Future<RecordMetadata> send(ProducerRecord<K,V> record) {
        assert producer != null;
        return producer.send(record);
    }

    public void send(ProducerRecord<K,V> record, Callback callback) {
        assert producer != null;
        producer.send(record, callback);
    }

    public void flush() {
        assert producer != null;
        producer.flush();
    }

    public void close(long timeout, TimeUnit timeUnit) {
        assert producer != null;
        producer.close(timeout, timeUnit);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        JKafkaProducer<String, String> producer = new JKafkaProducer<>(props);

        String topic = "JK-Message";

        Gson gson = new Gson();
        // 定时产生消息
        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            KafkaMessage kafkaMessage = KafkaMessage.createKafkaMessage();
            String msg = gson.toJson(kafkaMessage);
            producer.send(new ProducerRecord<>(topic, msg), (metadata, exp) -> {
                if (exp != null) {
                    exp.printStackTrace();
                } else {
                    System.out.println("topic = " + metadata.topic() + ", offset = " + metadata.offset() + ", partition = " + metadata.partition());
                }
            });
        }, 100, 500, TimeUnit.MILLISECONDS);
    }

}
