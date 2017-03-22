package com.sdu.jstorm.topology.group.bolt;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.logging.log4j.util.Strings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Word Sum Bolt
 *
 * @author hanhan.zhang
 * */
public class WordSumBolt extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordSumBolt.class);

    private OutputCollector _collector;

    private Cache<String, AtomicInteger> _wordCache;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
        this._wordCache = CacheBuilder.newBuilder()
                                        .maximumSize(1024)
                                        .expireAfterWrite(3, TimeUnit.SECONDS)
                                        .removalListener((removalNotification) -> {
                                            String key = (String) removalNotification.getKey();
                                            AtomicInteger sum = (AtomicInteger) removalNotification.getValue();
                                            LOGGER.info("word sum result : [{} , {}]", key, sum.get());
                                        })
                                        .build();
    }

    @Override
    public void execute(Tuple input) {
        try {
            String word = input.getString(0);
            int count = input.getInteger(1);
            if (Strings.isEmpty(word)) {
                return;
            }
            AtomicInteger counter = this._wordCache.getIfPresent(word);
            if (counter == null) {
                this._wordCache.put(word, new AtomicInteger(count));
            } else {
                counter.addAndGet(count);
            }
            this._collector.ack(input);
        } catch (Exception e) {
            this._collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
