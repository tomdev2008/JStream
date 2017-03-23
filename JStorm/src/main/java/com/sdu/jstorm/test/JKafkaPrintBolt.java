package com.sdu.jstorm.test;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class JKafkaPrintBolt extends BaseBasicBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(JKafkaPrintBolt.class);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String userId = input.getStringByField("userId");
        String action = input.getStringByField("action");
        String timestamp = input.getStringByField("timestamp");

        LOGGER.info("消息: userId = {}, action = {}, timestamp = {}", userId, action, timestamp);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
