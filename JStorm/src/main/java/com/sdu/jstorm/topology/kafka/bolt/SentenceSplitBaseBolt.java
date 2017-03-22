package com.sdu.jstorm.topology.kafka.bolt;

import com.google.common.base.Strings;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Out Tuple与Input Tuple自动建立锚定关系
 *
 * @author hanhan.zhang
 * */
public class SentenceSplitBaseBolt implements IBasicBolt {

    /**sentence receive counter*/
    private CountMetric countMetric;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        countMetric = new CountMetric();
        /**
         * storm ui展示统计tuple接收量
         * */
        context.registerMetric("receiver.counter", countMetric, 60);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String msg = input.getString(0);
        if (!Strings.isNullOrEmpty(msg)) {
            countMetric.incrBy(1);
            String []words = msg.split(",");
            List<Object> outputs = Arrays.stream(words)
                                        .map(word -> new Values(word, 1))
                                        .collect(Collectors.toList());
            collector.emit(outputs);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "number"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
