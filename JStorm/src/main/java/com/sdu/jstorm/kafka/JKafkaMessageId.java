package com.sdu.jstorm.kafka;

import com.sdu.jstorm.tuple.JStreamTuple;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * @author hanhan.zhang
 * */
public class JKafkaMessageId {

    private transient TopicPartition topicPart;
    private transient long offset;
    private transient int numFails = 0;

    @Setter
    @Getter
    private JStreamTuple tuple;

    // true if the record was emitted using a form of collector.emit(...).
    // false when skipping null tuples as configured by the user in KafkaSpoutConfig
    @Getter
    @Setter
    private boolean emitted;

    public JKafkaMessageId(ConsumerRecord<?, ?> consumerRecord) {
        this(consumerRecord, true);
    }

    public JKafkaMessageId(ConsumerRecord<?, ?> consumerRecord, boolean emitted) {
        this(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(), emitted);
    }

    public JKafkaMessageId(TopicPartition topicPart, long offset) {
        this(topicPart, offset, true);
    }

    public JKafkaMessageId(TopicPartition topicPart, long offset, boolean emitted) {
        this.topicPart = topicPart;
        this.offset = offset;
        this.emitted = emitted;
    }


    public long offset() {
        return offset;
    }

    public int numFails() {
        return numFails;
    }

    public void incrementNumFails() {
        ++numFails;
    }

    public TopicPartition getTopicPartition() {
        return topicPart;
    }

    public String getMetadata(Thread currThread) {
        return "{" +
                "topic-partition=" + topicPart +
                ", offset=" + offset +
                ", numFails=" + numFails +
                ", thread='" + currThread.getName() + "'" +
                '}';
    }

    @Override
    public String toString() {
        return "{" +
                "topic-partition=" + topicPart +
                ", offset=" + offset +
                ", numFails=" + numFails +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JKafkaMessageId messageId = (JKafkaMessageId) o;
        if (offset != messageId.offset) {
            return false;
        }
        return topicPart.equals(messageId.topicPart);
    }

    @Override
    public int hashCode() {
        int result = topicPart.hashCode();
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

}
