package com.sdu.jstorm.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.jstorm.common.operation.TupleGenerator;
import org.apache.storm.generated.Grouping;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fixed Cycle Spout
 *
 * @author hanhan.zhang
 * */
public class FixedCycleSpout implements IRichSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixedCycleSpout.class);

    // 是否为直接流
    private boolean _direct;
    // 流名称
    private String _streamId;
    private Fields _fields;


    // key = msgId, _value = sending tuple
    private Map<String, List<Object>> _pendingTuple;

    private SpoutOutputCollector _collector;
    private CountMetric _sendMetric;
    private CountMetric _failMetric;

    // consume task set(下游消费者)
    private AtomicInteger _consumeTaskIndex = new AtomicInteger(0);
    private List<Integer> _consumeTaskIdList;

    // 数据源
    private TupleGenerator _tupleGenerator;

    public FixedCycleSpout(Fields _fields, TupleGenerator tupleGenerator) {
        this(Utils.DEFAULT_STREAM_ID, false, _fields, tupleGenerator);
    }

    public FixedCycleSpout(String _streamId, boolean _direct, Fields _fields, TupleGenerator tupleGenerator) {
        this._streamId = _streamId;
        this._direct = _direct;
        this._fields = _fields;
        this._tupleGenerator = tupleGenerator;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _pendingTuple = Maps.newHashMap();

        // 统计信息
        this._sendMetric = context.registerMetric("cycle.spout.send.tuple.metric", new CountMetric(), 60);
        this._failMetric = context.registerMetric("cycle.spout.fail.tuple.metric", new CountMetric(), 60);
        this._collector = collector;

        // 直接流需要下游消费者
        if (this._direct) {
            this._consumeTaskIdList = Lists.newLinkedList();
            Map<String, Map<String, Grouping>> consumeTargets = context.getThisTargets();
            if (consumeTargets != null && !consumeTargets.isEmpty()) {
                // streamId = this._streamId
                consumeTargets.forEach((streamId, target) -> {
                    if (target != null && !target.isEmpty()) {
                        // componentId = consume target component Id
                        target.forEach((componentId, group) -> {
                            if (group.is_set_direct()) {
                                this._consumeTaskIdList.addAll(context.getComponentTasks(componentId));
                            }
                        });
                    }
                });
            }
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
//        try {
//            TimeUnit.MILLISECONDS.sleep(10);
//        } catch (Exception e) {
//
//        }
        this._sendMetric.incr();
        List<Object> sendTuple = this._tupleGenerator.generator();
        String msgId = UUID.randomUUID().toString();
        sendTuple(msgId, sendTuple);
    }

    @Override
    public void ack(Object msgId) {
        String msgIdStr = (String) msgId;
        LOGGER.debug("ack tuple message id {} .", msgIdStr);
        this._pendingTuple.remove(msgIdStr);
    }

    @Override
    public void fail(Object msgId) {
        this._failMetric.incr();
        String msgIdStr = (String) msgId;
        LOGGER.debug("fail tuple message id {} .", msgIdStr);
        sendTuple(msgIdStr, this._pendingTuple.get(msgIdStr));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._streamId, this._direct, this._fields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    protected void sendTuple(String msgId, List<Object> tuple) {
        this._pendingTuple.put(msgId, tuple);
        if (this._direct) {
            if (this._consumeTaskIdList == null || this._consumeTaskIdList.isEmpty()) {
                throw new IllegalStateException("direct task is empty !");
            }
            // 错误:如果向下游的每个Task发送数据会抛NullPointException,参见官网:
            // http://storm.apache.org/releases/1.0.2/Troubleshooting.html
//            this._consumeTaskIdList.forEach(taskId ->
//                    this._collector.emitDirect(taskId, this._streamId, tuple, msgId));
            // 正确:向下游消费者其中一个Task发送消息
            int curr = this._consumeTaskIndex.getAndIncrement() % this._consumeTaskIdList.size();
            this._collector.emitDirect(curr, this._streamId, tuple, msgId);
        } else {
            this._collector.emit(this._streamId, tuple, msgId);
        }
    }
}
