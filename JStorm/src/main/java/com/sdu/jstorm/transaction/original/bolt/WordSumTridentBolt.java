package com.sdu.jstorm.transaction.original.bolt;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.topology.BatchInfo;
import org.apache.storm.trident.topology.ITridentBatchBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Word Sum Trident Bolt
 *
 * @author hanhan.zhang
 * */
public class WordSumTridentBolt implements ITridentBatchBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordSumTridentBolt.class);

    private BatchOutputCollector _collector;

    private Cache<String, AtomicInteger> _wordCache;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector) {
        this._collector = collector;
        this._wordCache = CacheBuilder.newBuilder()
                                        .maximumSize(1024)
                                        .expireAfterWrite(3, TimeUnit.SECONDS)
                                        .removalListener((removalNotification) -> {
                                            String key = (String) removalNotification.getKey();
                                            AtomicInteger sum = (AtomicInteger) removalNotification.getValue();
                                            LOGGER.info("单词求和结果: [{} , {}]", key, sum.get());
                                        })
                                        .build();
    }

    @Override
    public void execute(BatchInfo batchInfo, Tuple tuple) {
        try {
            String word = tuple.getStringByField("word");
            int number = tuple.getIntegerByField("number");
            AtomicInteger counter = this._wordCache.getIfPresent(word);
            if (counter == null) {
                this._wordCache.put(word, new AtomicInteger(number));
            } else {
                counter.addAndGet(number);
            }
        } catch (Exception e) {
            this._collector.reportError(e);
        }
    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {

    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        return null;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
