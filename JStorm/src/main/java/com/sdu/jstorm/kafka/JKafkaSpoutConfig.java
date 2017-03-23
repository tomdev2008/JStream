package com.sdu.jstorm.kafka;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collection;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class JKafkaSpoutConfig<K, V> {

    public int getMaxRetryTimes() {
        return 0;
    }

    public JTranslator<K, V> getKafkaTranslator() {
        return null;
    }

    public long getPollTimeoutMs() {
        return 0L;
    }

    public long getOffsetsCommitPeriodMs() {
        return 0L;
    }

    public boolean isAutoCommitConsumeOffset() {
        return false;
    }

    public String getConsumerGroupId() {
        return null;
    }

    public Collection<String> getSubscribeTopic() {
        return null;
    }

    public Deserializer<K> getKeyDeserializer() {
        return null;
    }

    public Deserializer<V> getValueDeserializer() {
        return null;
    }

    public Map<String, Object> getKafkaProps() {
        return null;
    }

    public ConsumeOffsetStrategy getConsumeOffsetStrategy() {
        return null;
    }

    public static enum ConsumeOffsetStrategy {
        EARLIEST, LATEST, UNCOMMITTED_EARLIEST, UNCOMMITTED_LATEST
    }

}
