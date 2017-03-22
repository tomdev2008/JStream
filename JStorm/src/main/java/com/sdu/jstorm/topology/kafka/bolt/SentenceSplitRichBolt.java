package com.sdu.jstorm.topology.kafka.bolt;

import com.google.common.base.Strings;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * kafka message : sentence ---> word
 *
 * {@link OutputCollector}组件(继承{@link IOutputCollector}):
   1: 用于{@link IRichBolt}组件发送{@link Tuple}
   2: 向ACK上报消息消费状态(ack(tuple)或fail(tuple))

 * {@link OutputFieldsDeclarer}组件:
   1: 声明发送{@link Tuple}的消息流字段名

 * Note:
    Out Tuple与Input Tuple不会自动建立锚定关系
 *
 * */
public class SentenceSplitRichBolt implements IRichBolt {

    private OutputCollector _collector;

    /**sentence receive counter*/
    private CountMetric countMetric;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        countMetric = new CountMetric();
        /**
         * storm ui展示统计tuple接收量
         * */
        context.registerMetric("receiver.counter", countMetric, 60);
    }

    @Override
    public void execute(Tuple input) {
        try {
            String msg = input.getString(0);
            if (!Strings.isNullOrEmpty(msg)) {
                countMetric.incrBy(1);
                String []words = msg.split("\t");
                List<Object> outputs = Arrays.stream(words)
                                            .map(word -> new Values(word, 1))
                                            .collect(Collectors.toList());
                _collector.emit(outputs);
                _collector.ack(input);
            }

        } catch (Exception e) {
            _collector.fail(input);
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
