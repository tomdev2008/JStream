package com.sdu.jstorm.group.bolt;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.sdu.jstorm.utils.Const;
import org.apache.storm.generated.Grouping;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sentence Split Bolt
 *
 * @author hanhan.zhang
 * */
public class SentenceSplitBolt implements IRichBolt {

    private OutputCollector _collector;

    // 统计信息
    private CountMetric _ackMetric;
    private CountMetric _failMetric;

    private String _separator;

    private boolean _direct;

    private String _streamId;

    // consume task set(下游消费者Task集合)
    private AtomicInteger _consumeTaskIndex = new AtomicInteger(0);
    private List<Integer> _consumeTaskList;


    public SentenceSplitBolt() {
        this(Utils.DEFAULT_STREAM_ID, false);
    }

    public SentenceSplitBolt(String _streamId, boolean _direct) {
        this._streamId = _streamId;
        this._direct = _direct;
    }

    /**
     * @param context
     *          1: Register Metric
     *          2: Next Bolt Message
     * @param collector (thread-safe)
     *          1: Emit Tuple
     *          2: Ack/Fail Tuple
     * */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;

        // register metric for monitor
        this._ackMetric = context.registerMetric("sentence.split.ack.metric", new CountMetric(), 60);
        this._failMetric = context.registerMetric("sentence.split.fail.metric", new CountMetric(), 60);

        this._separator = (String) stormConf.get(Const.SEPARATOR);

        // 下游消费者Task
        if (this._direct) {
            if (this._consumeTaskList == null) {
                this._consumeTaskList = Lists.newLinkedList();
            }
            Map<String, Map<String, Grouping>> consumeTarget = context.getThisTargets();
            consumeTarget.forEach((streamId, target) ->
                target.forEach((componentId, group) -> {
                    if (group.is_set_direct()) {
                        this._consumeTaskList.addAll(context.getComponentTasks(componentId));
                    }
                })
            );
        }

    }

    @Override
    public void execute(Tuple input) {
        try {
            String sentence = input.getString(0);
            if (Strings.isNullOrEmpty(sentence)) {
                return;
            }
            String []fields = sentence.split(_separator);
            for (String field : fields) {
                if (this._direct) {
                    int curr = this._consumeTaskIndex.getAndIncrement() % this._consumeTaskList.size();
                    this._collector.emitDirect(curr, this._streamId, input, new Values(field, 1));
                } else {
                    this._collector.emit(this._streamId, input, new Values(field, 1));
                }
            }
            this._collector.ack(input);
            this._ackMetric.incr();
        } catch (Exception e) {
            this._collector.fail(input);
            this._failMetric.incr();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._streamId, this._direct, new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
