package com.sdu.jstorm.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 *
 * */
public interface JKafkaSpoutRetryService {

    boolean retainAll(Collection<TopicPartition> topicPartitions);

    boolean schedule(JKafkaMessageId msgId);
}
