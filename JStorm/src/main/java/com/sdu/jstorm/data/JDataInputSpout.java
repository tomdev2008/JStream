package com.sdu.jstorm.data;

import com.google.common.collect.Sets;
import com.sdu.jstorm.data.internal.JDataSource;
import com.sdu.jstorm.kafka.internal.JTimer;
import com.sdu.jstorm.translator.JDataTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author hanhan.zhang
 * */
public class JDataInputSpout<T> implements IRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDataInputSpout.class);

    private static final long TIMER_DELAY_MS = 500;

    private JDataInputConfig<T> dataInputConfig;

    private SpoutOutputCollector collector;

    private JDataSource<T> dataSource;
    private boolean autoCommit;

    // 数据统计
    private static final String DATA_EMIT = "data.emit";
    private CountMetric DATE_EMIT_METRIC;
    private static final String DATA_EMIT_ACK = "data.emit.ack";
    private CountMetric DATA_EMIT_ACK_METRIC;

    //
    private JTimer commitTimer;
    private Set<JDataMessageId> waitingAcked;
    private Set<JDataMessageId> emited;

    public JDataInputSpout(JDataInputConfig<T> dataInputConfig) {
        this.dataInputConfig = dataInputConfig;
        this.dataSource = dataInputConfig.getDataSource();
        this.autoCommit = dataInputConfig.isAutoCommit();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        DATE_EMIT_METRIC = context.registerMetric(DATA_EMIT, new CountMetric(), 60);
        DATA_EMIT_ACK_METRIC = context.registerMetric(DATA_EMIT_ACK, new CountMetric(), 60);

        emited = Sets.newHashSet();
        waitingAcked = Sets.newHashSet();
        if (!autoCommit) {
            commitTimer = new JTimer(TIMER_DELAY_MS, dataInputConfig.getAutoCommitPeriodMs(), TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void activate() {
        this.dataSource.start();
    }

    @Override
    public void nextTuple() {
        if (isCommit()) {
            commitData();
        }
        T data = this.dataSource.nextData();
        JDataTranslator<T> translator = dataInputConfig.getDataTranslator();
        JStreamTuple tuple = translator.apply(data);
        if (tuple != null) {
            JDataMessageId dataMessageId = new JDataMessageId(translator.getCommitKey(data) ,tuple);
            collector.emit(tuple.tupleStream(), tuple.streamTuple(), dataMessageId);
            emited.add(dataMessageId);
            DATE_EMIT_METRIC.incr();
        }
    }

    private boolean isCommit() {
        return !autoCommit && commitTimer.isExpiredResetOnTrue();
    }

    private void commitData() {
        if (waitingAcked != null && waitingAcked.size() > 0) {
            Iterator<JDataMessageId> iterator = waitingAcked.iterator();
            while (iterator.hasNext()) {
                JDataMessageId dataMessageId = iterator.next();
                iterator.remove();
                if (dataMessageId != null) {
                    this.dataSource.commit(dataMessageId.getCommitKey());
                }
            }
        }
    }

    @Override
    public void deactivate() {
        shutdown();
    }

    @Override
    public void close() {
        shutdown();
    }

    private void shutdown() {
        dataSource.close();
    }

    @Override
    public void ack(Object msgId) {
        if (msgId.getClass() == JDataMessageId.class) {
            JDataMessageId dataMessageId = (JDataMessageId) msgId;
            if (emited.contains(dataMessageId)) {
                emited.remove(dataMessageId);
                waitingAcked.add(dataMessageId);
                DATA_EMIT_ACK_METRIC.incr();
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId.getClass() == JDataMessageId.class) {
            JDataMessageId dataMessageId = (JDataMessageId) msgId;
            dataMessageId.incrementFailure();
            if (dataMessageId.getFailure() >= dataInputConfig.getMaxRetryTimes()) {
                LOGGER.info("消息失败重试已到上限, 丢弃: key = {}", dataMessageId.getCommitKey());
                ack(dataMessageId);
            } else {
                JStreamTuple tuple = dataMessageId.getTuple();
                collector.emit(tuple.tupleStream(), tuple.streamTuple(), dataMessageId);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        JDataTranslator<T> translator = this.dataInputConfig.getDataTranslator();
        translator.streams().forEach(streamName ->
                declarer.declareStream(streamName, translator.isDirectFor(streamName), translator.getFieldsFor(streamName))
        );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
