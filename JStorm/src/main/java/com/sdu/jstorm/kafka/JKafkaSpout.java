package com.sdu.jstorm.kafka;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.jstorm.kafka.internal.JDefaultKafkaConsumerFactory;
import com.sdu.jstorm.kafka.internal.JKafkaConsumerFactory;
import com.sdu.jstorm.kafka.internal.JOffsetManager;
import com.sdu.jstorm.kafka.internal.JTimer;
import com.sdu.jstorm.translator.JKafkaTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.sdu.jstorm.kafka.JKafkaSpoutConfig.ConsumeOffsetStrategy.*;

/**
 * {@link JKafkaSpout}基于Kafka 0.10.0.0版本
 *
 * @author hanhan.zhang
 * */
public class JKafkaSpout<K, V> extends BaseRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(JKafkaSpout.class);

    // Kafka消息位置提交异常统计
    private static final String OFFSET_COMMIT_FAILURE = "kafka.offset.commit.failure";
    private CountMetric OFFSET_COMMIT_FAILURE_METRIC;
    private static final String KAFKA_MESSAGE_EMIT = "kafka.message.emit";
    private CountMetric KAFKA_MESSAGE_EMIT_METRIC;

    private static final long TIMER_DELAY_MS = 500;

    private SpoutOutputCollector collector;

    private transient volatile boolean initialized;

    // Kafka
    private JKafkaConsumerFactory<K, V> consumerFactory;
    private JKafkaSpoutConfig<K, V> spoutConfig;
    private KafkaConsumer<K, V> consumer;
    // 是否自动提交Kafka消息消费位置
    private boolean autoCommitOffset;
    private JTimer commitTimer;

    // 记录待提交Kafka消息的位置信息
    private transient Map<TopicPartition, JOffsetManager> waitingCommitOffset;
    // 已发送Kafka消息
    private transient Set<JKafkaMessageId> emitted;
    // 待发送Kafka消息
    private transient Iterator<ConsumerRecord<K, V>> waitingToEmit;

    public JKafkaSpout(JKafkaSpoutConfig<K, V> spoutConfig) {
        this(spoutConfig, new JDefaultKafkaConsumerFactory<>());
    }

    public JKafkaSpout(JKafkaSpoutConfig<K, V> spoutConfig, JKafkaConsumerFactory<K, V> consumerFactory) {
        this.spoutConfig = spoutConfig;
        this.consumerFactory = consumerFactory;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        initialized = false;

        //
        waitingCommitOffset = Maps.newHashMap();
        emitted = Sets.newHashSet();
        waitingToEmit = Collections.emptyIterator();

        this.collector = collector;

        this.autoCommitOffset = spoutConfig.isAutoCommitConsumeOffset();
        if (!this.autoCommitOffset) {
            commitTimer = new JTimer(TIMER_DELAY_MS, spoutConfig.getOffsetsCommitPeriodMs(), TimeUnit.MILLISECONDS);
        }

        // 统计信息
        OFFSET_COMMIT_FAILURE_METRIC = context.registerMetric(OFFSET_COMMIT_FAILURE, new CountMetric(), 60);
        KAFKA_MESSAGE_EMIT_METRIC = context.registerMetric(KAFKA_MESSAGE_EMIT, new CountMetric(), 60);

    }

    @Override
    public void activate() {
        consumer = consumerFactory.createConsumer(spoutConfig);
        consumer.subscribe(spoutConfig.getSubscribeTopic(), new JSpoutConsumerRebalanceListener());
        // Initial poll to get the consumer registration process going
        // JSpoutConsumerRebalanceListener will be called following this poll, upon partition registration
        consumer.poll(0);
    }

    @Override
    public void nextTuple() {
        if (initialized) {
            // Kafka消息消费Offset提交
            if (isCommitConsumeOffset()) {
                commitConsumeOffset();
            }

            boolean isEmit = isEmitKafkaMessage();

            if (!isEmit) {
                pollKafkaMessage();
            }

            if (isEmitKafkaMessage()) {
                emitKafkaMessage();
            }
        } else {
            LOGGER.debug("KafkaSpout not initialized. Not sending tuples until initialization completes");
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId.getClass() == JKafkaMessageId.class) {
            JKafkaMessageId kafkaMessageId = (JKafkaMessageId) msgId;
            if (emitted.contains(kafkaMessageId)) {
                emitted.remove(kafkaMessageId);
                if (!autoCommitOffset) {
                    JOffsetManager offsetManager = waitingCommitOffset.get(kafkaMessageId.getTopicPartition());
                    offsetManager.add(kafkaMessageId);
                }
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId.getClass() == JKafkaMessageId.class) {
            JKafkaMessageId kafkaMessageId = (JKafkaMessageId) msgId;
            if (!emitted.contains(kafkaMessageId)) {
                return;
            }
            kafkaMessageId.incrementNumFails();

            if (spoutConfig.getMaxRetryTimes() >= kafkaMessageId.numFails()) {
                LOGGER.debug("消息[{}]已达到失败重试次数限制, 丢弃该消息", kafkaMessageId);
                ack(kafkaMessageId);
            } else {
                JStreamTuple tuple = kafkaMessageId.getTuple();
                collector.emit(tuple.stream(), tuple.tuple(), kafkaMessageId);
                KAFKA_MESSAGE_EMIT_METRIC.incr();
            }
        }
    }

    private boolean isCommitConsumeOffset() {
        return !autoCommitOffset && commitTimer.isExpiredResetOnTrue();
    }

    // 提交Kafka消息消费位置
    private void commitConsumeOffset() {
        Map<TopicPartition, OffsetAndMetadata> needCommitTopicPartition = Maps.newHashMap();
        waitingCommitOffset.forEach((partition, offsetManager) -> {
            OffsetAndMetadata nextCommitOffset = offsetManager.findNextCommitOffset();
            if (nextCommitOffset != null) {
                needCommitTopicPartition.put(partition, nextCommitOffset);
            }
        });

        // 异步提交Kafka消费位置
        if (needCommitTopicPartition.size() > 0) {
            consumer.commitAsync(needCommitTopicPartition, new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        OFFSET_COMMIT_FAILURE_METRIC.incr();
                    } else {
                        // 修改JOffsetManager已提交的Offset记录
                        offsets.forEach((partition, metadata) -> {
                            LOGGER.info("Kafka消息消费位置提交成功, partition = {}, offset = {}", partition, metadata.offset());
                            JOffsetManager offsetManager = waitingCommitOffset.get(partition);
                            if (offsetManager != null) {
                                offsetManager.commit(metadata);
                            }
                        });
                    }
                }
            });
        }
    }


    private void pollKafkaMessage() {
        for (;;) {
            ConsumerRecords<K, V> consumerRecords = consumer.poll(spoutConfig.getPollTimeoutMs());
            if (consumerRecords != null && consumerRecords.partitions() != null) {
                List<ConsumerRecord<K, V>> waitingToEmitList = new LinkedList<>();
                consumerRecords.partitions().forEach(partition ->
                        waitingToEmitList.addAll(consumerRecords.records(partition))
                );
                waitingToEmit = waitingToEmitList.iterator();
                break;
            }
        }
    }

    private boolean isEmitKafkaMessage() {
        return waitingToEmit != null && waitingToEmit.hasNext();
    }

    private void emitKafkaMessage() {
        do {
            ConsumerRecord<K, V> consumerRecord = waitingToEmit.next();
            waitingToEmit.remove();
            // 发送Kafka消息
            JKafkaMessageId kafkaMsgId = new JKafkaMessageId(consumerRecord);
            TopicPartition tp = kafkaMsgId.getTopicPartition();
            if (waitingCommitOffset.containsKey(tp) && waitingCommitOffset.get(tp).contains(kafkaMsgId)) {
                // 消息已被确认
                LOGGER.trace("消息[{}]已被确认消费, 跳过该条消息", consumerRecord);
            } else if (emitted.contains(kafkaMsgId)) {
                LOGGER.trace("消息[{}]已被发送, 跳过该条消息", consumerRecord);
            } else {
                JStreamTuple tuple = spoutConfig.getKafkaTranslator().apply(consumerRecord);
                if (tuple != null) {
                    kafkaMsgId.setTuple(tuple);
                    collector.emit(tuple.stream(), tuple.tuple(), kafkaMsgId);
                    emitted.add(kafkaMsgId);
                }
                KAFKA_MESSAGE_EMIT_METRIC.incr();
            }
        } while (waitingToEmit.hasNext());
    }

    @Override
    public void close() {
        shutdown();
    }

    @Override
    public void deactivate() {
        shutdown();
    }

    private void shutdown() {
        if (!autoCommitOffset) {
            commitConsumeOffset();
        }
        consumer.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        JKafkaTranslator<K, V> translator = spoutConfig.getKafkaTranslator();
        translator.streams().forEach(streamName ->
            declarer.declareStream(streamName, false, translator.getFieldsFor(streamName))
        );
    }

    private class JSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            // KafkaConsumer.close(), the method is called
            if (!autoCommitOffset && initialized) {
                initialized = false;
                commitConsumeOffset();
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOGGER.info("Spout消息Kafka分区信息: [consumer-group={}, consumer={}, topic-partitions={}]",
                    spoutConfig.getConsumerGroupId(), consumer, partitions);
            initialize(partitions);
        }

        private void initialize(Collection<TopicPartition> partitions) {
            // 求交集, 保留该Spout需确认的TopicPartition
            if(!autoCommitOffset) {
                waitingCommitOffset.keySet().retainAll(partitions);
            }

            Set<TopicPartition> needEmitPartitions = Sets.newHashSet(partitions);
            Iterator<JKafkaMessageId> msgIterator = emitted.iterator();
            while (msgIterator.hasNext()) {
                JKafkaMessageId msgId = msgIterator.next();
                if (needEmitPartitions.contains(msgId.getTopicPartition())) {
                    continue;
                }
                msgIterator.remove();
            }

            // 消费位置确认
            partitions.forEach(topicPartition -> {
                OffsetAndMetadata metadata = consumer.committed(topicPartition);
                // 根据消费策略, 确认Kafka消息消费起始位置
                long fetchOffset = correctConsumeOffset(topicPartition, metadata);
                // 记录TopicPartition的消费位置
                recordTopicPartitionConsumeOffset(topicPartition, fetchOffset);
            });
            initialized = true;
        }

        private long correctConsumeOffset(TopicPartition partition, OffsetAndMetadata metadata) {
            long fetchOffset;
            JKafkaSpoutConfig.ConsumeOffsetStrategy strategy = spoutConfig.getConsumeOffsetStrategy();
            if (metadata != null) {
                switch (strategy) {
                    case EARLIEST:
                        consumer.seekToBeginning(Collections.singleton(partition));
                        fetchOffset = consumer.position(partition);
                        break;
                    case LATEST:
                        consumer.seekToEnd(Collections.singleton(partition));
                        fetchOffset = consumer.position(partition);
                        break;
                    default:
                        fetchOffset = metadata.offset() + 1;
                        consumer.seek(partition, fetchOffset);
                  }
                return fetchOffset;
            }
            if (strategy == EARLIEST || strategy == UNCOMMITTED_EARLIEST) {
                consumer.seekToBeginning(Collections.singleton(partition));
            } else if (strategy == LATEST || strategy == UNCOMMITTED_LATEST) {
                consumer.seekToEnd(Collections.singleton(partition));
            }
            fetchOffset = consumer.position(partition);

            return fetchOffset;
        }

        private void recordTopicPartitionConsumeOffset(TopicPartition partition, long offset) {
            if (!autoCommitOffset) {
                JOffsetManager offsetManager = waitingCommitOffset.get(partition);
                if (offsetManager == null) {
                    offsetManager = new JOffsetManager(partition, offset);
                    waitingCommitOffset.put(partition, offsetManager);
                }
            }
        }

    }
}
