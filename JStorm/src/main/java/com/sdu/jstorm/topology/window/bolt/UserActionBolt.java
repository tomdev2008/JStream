package com.sdu.jstorm.topology.window.bolt;

import com.google.common.collect.Lists;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User Action Bolt
 *
 * @author hanhan.zhang
 * */
public class UserActionBolt extends BaseRichBolt {

    private OutputCollector _collector;

    private String _stream;

    private boolean _direct;

    private Fields _fields;

    // consume task
    private AtomicInteger _curr;
    private List<Integer> _consumeTaskList;

    public UserActionBolt(String _stream, boolean _direct, Fields _fields) {
        this._stream = _stream;
        this._direct = _direct;
        this._fields = _fields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        if (this._direct) {
            this._consumeTaskList = Lists.newArrayList();
            this._curr = new AtomicInteger(0);
            Map<String, Map<String, Grouping>> consumeComponentMap = context.getThisTargets();
            consumeComponentMap.forEach((streamId, componentMap) ->
                componentMap.forEach((component, grouping) -> {
                    this._consumeTaskList.addAll(context.getComponentTasks(component));
                })
            );
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            String type = input.getStringByField("actionType");
            long time = input.getLongByField("actionTime");
            List<Object> sendTuple = new Values(type, time, 1);
            if (this._direct) {
                int index = this._curr.getAndIncrement();
                int taskId = index % this._consumeTaskList.size();
                this._collector.emitDirect(taskId, this._stream, input, sendTuple);
            } else {
                this._collector.emit(this._stream, input, sendTuple);
            }
        } catch (Exception e) {
            this._collector.fail(input);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._stream, this._direct, this._fields);
    }

}
