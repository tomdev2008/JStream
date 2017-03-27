package com.sdu.jstorm.aggregate;

import com.sdu.jstorm.translator.JWindowTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

/**
 * {@link org.apache.storm.topology.WindowedBoltExecutor}
 *
 * @author hanhan.zhang
 * */
public class JStreamWindowBolt extends BaseWindowedBolt {

    private JWindowTranslator translator;

    private OutputCollector collector;

    public JStreamWindowBolt(JWindowTranslator translator) {
        this.translator = translator;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        List<JStreamTuple> streamTuples = translator.apply(inputWindow);
        // 不需对InputTuple消费确认, WindowedBoltExecutor对已过期的InputTuple已消费确认
        streamTuples.forEach(tuple ->
            collector.emit(tuple.stream(), tuple.tuple())
        );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        translator.streams().forEach(stream ->
            declarer.declareStream(stream, translator.getFieldsFor(stream))
        );
    }
}
