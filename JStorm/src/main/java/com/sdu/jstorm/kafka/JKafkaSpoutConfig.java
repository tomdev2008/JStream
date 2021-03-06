package com.sdu.jstorm.kafka;

import com.sdu.jstorm.translator.JKafkaTranslator;
import lombok.Setter;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class JKafkaSpoutConfig<K, V> implements Serializable {

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
    private JKafkaTranslator<K, V> translator;

    public int getMaxRetryTimes() {
        return retryTimes <= 0 ? MAX_RETRY_TIMES : retryTimes;
    }

    public JKafkaTranslator<K, V> getKafkaTranslator() {
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
