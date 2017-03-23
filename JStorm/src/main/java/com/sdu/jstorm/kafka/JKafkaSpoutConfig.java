package com.sdu.jstorm.kafka;

import lombok.Setter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class JKafkaSpoutConfig<K, V> {

    private static final int MAX_RETRY_TIMES = 3;
    private static final long MAX_POLL_TIMEOUT_MS = 1000L;
    private static final long MAX_COMMIT_PERIOD_MS = 2000;

    @Setter
    private int retryTimes;
    @Setter
    private long pollTimeoutMs;
    @Setter
    private boolean autoCommit;
    @Setter
    private long commitPeriodMs;
    @Setter
    private String groupId;
    @Setter
    private Collection<String> topics;
    @Setter
    private Map<String, Object> kafkaProps;
    @Setter
    private ConsumeOffsetStrategy consumeStrategy;
    @Setter
    private Deserializer keyDeserializer;
    @Setter
    private Deserializer valueDeserializer;
    @Setter
    private JTranslator<K, V> translator;

    public int getMaxRetryTimes() {
        return retryTimes <= 0 ? MAX_RETRY_TIMES : retryTimes;
    }

    public JTranslator<K, V> getKafkaTranslator() {
        return translator;
    }

    public long getPollTimeoutMs() {
        return pollTimeoutMs <= 0 ? MAX_POLL_TIMEOUT_MS : pollTimeoutMs;
    }

    public long getOffsetsCommitPeriodMs() {
        return commitPeriodMs <= 0 ? MAX_COMMIT_PERIOD_MS : commitPeriodMs;
    }

    public boolean isAutoCommitConsumeOffset() {
        return autoCommit;
    }

    public String getConsumerGroupId() {
        return groupId;
    }

    public Collection<String> getSubscribeTopic() {
        return topics;
    }

    public Deserializer getKeyDeserializer() {
        return keyDeserializer == null ? new StringDeserializer() : keyDeserializer;
    }

    public Deserializer getValueDeserializer() {
        return valueDeserializer == null ? new StringDeserializer() : valueDeserializer;
    }

    public Map<String, Object> getKafkaProps() {
        return kafkaProps;
    }

    public ConsumeOffsetStrategy getConsumeOffsetStrategy() {
        return consumeStrategy == null ? ConsumeOffsetStrategy.LATEST : consumeStrategy;
    }

    public static enum ConsumeOffsetStrategy {
        EARLIEST, LATEST, UNCOMMITTED_EARLIEST, UNCOMMITTED_LATEST
    }

}
