package com.sdu.jstorm.group.router.bolt;

import com.google.common.collect.Lists;
import com.sdu.jstorm.group.router.help.StreamDesc;
import org.apache.logging.log4j.util.Strings;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Multiple Stream Bolt
 *
 * @author hanhan.zhang
 * */
public class MultiStreamBolt extends BaseRichBolt {

    private OutputCollector _collector;

    private List<StreamDesc> _streamDescList;

    // task set consume the bolt
    private AtomicInteger _consumeTaskIndex = new AtomicInteger(0);
    private List<Integer> _consumeTaskIdList;

    public MultiStreamBolt(List<StreamDesc> streamDescList) {
        this._streamDescList = streamDescList;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;

        // get consume executor task id
        Map<String, Map<String, Grouping>> consumeTaskMap = context.getThisTargets();
        consumeTaskMap.forEach((streamId, consumeTarget) -> {
            if (consumeTarget != null && !consumeTarget.isEmpty()) {
                consumeTarget.forEach((componentId, group) -> {
                    if (group.is_set_direct() && Strings.isNotEmpty(componentId)) {
                        List<Integer> taskIdList = context.getComponentTasks(componentId);
                        if (taskIdList == null || taskIdList.isEmpty()) {
                            throw new IllegalStateException("component '" + componentId + "' task is zero !");
                        }
                        if (this._consumeTaskIdList == null) {
                            this._consumeTaskIdList = Lists.newLinkedList();
                        }
                        this._consumeTaskIdList.addAll(taskIdList);
                    }
                });
            }
        });
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        if (Strings.isEmpty(str)) {
            return;
        }
        this._streamDescList.forEach(streamDesc -> {
            if (streamDesc.interest(str)) {
                if (streamDesc.isDirect()) {
                    int curr = this._consumeTaskIndex.getAndIncrement() % this._consumeTaskIdList.size();
                    this._collector.emitDirect(curr, streamDesc.getStreamId(), input, new Values(str));
                } else {
                    this._collector.emit(streamDesc.getStreamId(), input, new Values(str));
                }
            }
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this._streamDescList.forEach(streamDesc ->
            declarer.declareStream(streamDesc.getStreamId(), streamDesc.isDirect(), streamDesc.getFields())
        );
    }
}
