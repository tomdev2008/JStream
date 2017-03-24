package com.sdu.jstorm.group.direct;

import com.google.common.collect.Lists;
import com.sdu.jstorm.translator.JStreamTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import com.sdu.jstorm.utils.CollectionUtil;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Storm消息'Direct'分组方式使用条件:
 *
 *  1: {@link JStreamDirectBolt}发送数据: {@link OutputCollector#emitDirect(int, String, Tuple, List)}
 *
 *  2: {@link JStreamDirectBolt}声明为直接流
 *
 * @author hanhan.zhang
 * */
public class JStreamDirectBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(JStreamDirectBolt.class);

    // 下游直接消费的Bolt的编号
    private List<Integer> directConsumeComponent;

    private JStreamTranslator translator;

    private OutputCollector collector;

    public JStreamDirectBolt(JStreamTranslator translator) {
        this.translator = translator;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        if (directConsumeComponent == null) {
            directConsumeComponent = Lists.newArrayList();
        }
        Map<String, Map<String, Grouping>> consumeComponentInfoMap = context.getThisTargets();
        if (consumeComponentInfoMap != null && consumeComponentInfoMap.size() > 0) {
            // key = 消息流标识, value = [key = 组件标识, value = 消息分组]
            consumeComponentInfoMap.forEach((subscribeStreamName, componentSubscribeGroup) -> {
                if (componentSubscribeGroup != null) {
                    componentSubscribeGroup.forEach((component, group) -> {
                        if (group.is_set_direct()) {
                            List<Integer> taskIds = context.getComponentTasks(component);
                            LOGGER.info("消息直接分组：Bolt = {}, TaskId = {}, stream = {}", component, taskIds, subscribeStreamName);
                            directConsumeComponent.addAll(taskIds);
                        }
                    });
                }
            });
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            List<JStreamTuple> streamTuples = translator.apply(input);
            if (CollectionUtil.isNotEmpty(streamTuples)) {
                Collections.shuffle(directConsumeComponent);
                int taskId = directConsumeComponent.get(0);
                streamTuples.forEach(streamTuple ->
                        collector.emitDirect(taskId, streamTuple.stream(), input, streamTuple.tuple())
                );
            }
            collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 必须声明为直接流
        translator.streams().forEach(streamName ->
            declarer.declareStream(streamName, true, translator.getFieldsFor(streamName))
        );
    }
}
