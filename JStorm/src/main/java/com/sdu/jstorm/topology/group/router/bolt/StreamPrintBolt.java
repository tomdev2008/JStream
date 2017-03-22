package com.sdu.jstorm.topology.group.router.bolt;

import org.apache.logging.log4j.util.Strings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Stream Print Bolt
 *
 * @author hanhan.zhang
 * */
public class StreamPrintBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamPrintBolt.class);

    private OutputCollector _collector;

    private String _interestStream;

    private boolean _debug;

    public StreamPrintBolt(String _interestStream, boolean _debug) {
        this._interestStream = _interestStream;
        this._debug = _debug;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        if (Strings.isEmpty(str)) {
            return;
        }
        if (this._debug) {
            LOGGER.info("Interest Stream {} , Receive Input : {} " , this._interestStream, str);
        }
        this._collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
