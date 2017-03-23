package com.sdu.jstorm.kafka.internal;

import com.sdu.jstorm.kafka.JKafkaMessageId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * {@link JOffsetManager}非线程安全
 *
 * @author hanhan.zhang
 * */
public class JOffsetManager {
    private static final Logger LOG = LoggerFactory.getLogger(JOffsetManager.class);

    private static final Comparator<JKafkaMessageId> OFFSET_COMPARATOR = new OffsetComparator();

    private final TopicPartition tp;

    /* First offset to be fetched. It is either set to the beginning, end, or to the first uncommitted offset.
    * Initial value depends on offset strategy. See KafkaSpoutConsumerRebalanceListener */
    private final long initialFetchOffset;

    // Last offset committed to Kafka. Initially it is set to fetchOffset - 1
    private long committedOffset;

    // Acked messages sorted by ascending order of offset
    private final NavigableSet<JKafkaMessageId> ackedMsgs = new TreeSet<>(OFFSET_COMPARATOR);

    public JOffsetManager(TopicPartition tp, long initialFetchOffset) {
        this.tp = tp;
        this.initialFetchOffset = initialFetchOffset;
        this.committedOffset = initialFetchOffset - 1;
    }

    public void add(JKafkaMessageId msgId) {          // O(Log N)
        ackedMsgs.add(msgId);
    }

    /**
     * An offset is only committed when all records with lower offset have been
     * acked. This guarantees that all offsets smaller than the committedOffset
     * have been delivered.
     *
     * @return the next OffsetAndMetadata to commit, or null if no offset is
     * ready to commit.
     */
    public OffsetAndMetadata findNextCommitOffset() {
        boolean found = false;
        long currOffset;
        long nextCommitOffset = committedOffset;
        JKafkaMessageId nextCommitMsg = null;     // this is a convenience variable to make it faster to create OffsetAndMetadata

        for (JKafkaMessageId currAckedMsg : ackedMsgs) {  // complexity is that of a linear scan on a TreeMap
            if ((currOffset = currAckedMsg.offset()) == nextCommitOffset + 1) {            // found the next offset to commit
                found = true;
                nextCommitMsg = currAckedMsg;
                nextCommitOffset = currOffset;
            } else if (currAckedMsg.offset() > nextCommitOffset + 1) {    // offset found is not continuous to the offsets listed to go in the next commit, so stop search
                LOG.debug("topic-partition [{}] has non-continuous offset [{}]. It will be processed in a subsequent batch.", tp, currOffset);
                break;
            } else {
                //Received a redundant ack. Ignore and continue processing.
                LOG.warn("topic-partition [{}] has unexpected offset [{}]. Current committed Offset [{}]",
                        tp, currOffset, committedOffset);
            }
        }

        OffsetAndMetadata nextCommitOffsetAndMetadata = null;
        if (found) {
            nextCommitOffsetAndMetadata = new OffsetAndMetadata(nextCommitOffset, nextCommitMsg.getMetadata(Thread.currentThread()));
            LOG.debug("topic-partition [{}] has offsets [{}-{}] ready to be committed", tp, committedOffset + 1, nextCommitOffsetAndMetadata.offset());
        } else {
            LOG.debug("topic-partition [{}] has NO offsets ready to be committed", tp);
        }
        LOG.trace("{}", this);
        return nextCommitOffsetAndMetadata;
    }

    /**
     * Marks an offset has committed. This method has side effects - it sets the
     * internal state in such a way that future calls to
     * {@link #findNextCommitOffset()} will return offsets greater than the
     * offset specified, if any.
     *
     * @param committedOffset offset to be marked as committed
     * @return Number of offsets committed in this commit
     */
    public long commit(OffsetAndMetadata committedOffset) {
        long preCommitCommittedOffsets = this.committedOffset;
        long numCommittedOffsets = committedOffset.offset() - this.committedOffset;
        this.committedOffset = committedOffset.offset();
        for (Iterator<JKafkaMessageId> iterator = ackedMsgs.iterator(); iterator.hasNext();) {
            if (iterator.next().offset() <= committedOffset.offset()) {
                iterator.remove();
            } else {
                break;
            }
        }
        LOG.trace("{}", this);

        LOG.debug("Committed offsets [{}-{} = {}] for topic-partition [{}].",
                preCommitCommittedOffsets + 1, this.committedOffset, numCommittedOffsets, tp);

        return numCommittedOffsets;
    }

    public long getCommittedOffset() {
        return committedOffset;
    }

    public boolean isEmpty() {
        return ackedMsgs.isEmpty();
    }

    public boolean contains(ConsumerRecord record) {
        return contains(new JKafkaMessageId(record));
    }

    public boolean contains(JKafkaMessageId msgId) {
        return ackedMsgs.contains(msgId);
    }

    @Override
    public String toString() {
        return "OffsetManager{"
                + "topic-partition=" + tp
                + ", fetchOffset=" + initialFetchOffset
                + ", committedOffset=" + committedOffset
                + ", ackedMsgs=" + ackedMsgs
                + '}';
    }

    private static class OffsetComparator implements Comparator<JKafkaMessageId> {

        @Override
        public int compare(JKafkaMessageId m1, JKafkaMessageId m2) {
            return m1.offset() < m2.offset() ? -1 : m1.offset() == m2.offset() ? 0 : 1;
        }
    }
}
