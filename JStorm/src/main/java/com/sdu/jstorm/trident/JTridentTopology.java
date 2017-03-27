package com.sdu.jstorm.trident;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.sdu.jstorm.data.JDataInputConfig;
import com.sdu.jstorm.data.JDataInputSpout;
import com.sdu.jstorm.test.JInputData;
import com.sdu.jstorm.test.JTestDataSource;
import com.sdu.jstorm.translator.JDataTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import com.sdu.jstorm.utils.JTestUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.trident.state.RedisMapState;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class JTridentTopology {

    public static void main(String[] args) {

        TridentTopology tridentTopology = new TridentTopology();

        // Spout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                                                    new Values("the cow jumped over the moon"),
                                                    new Values("the man went to the store and bought some candy"),
                                                    new Values("four score and seven years ago"),
                                                    new Values("how many apples can you eat"));
        spout.setCycle(true);
        Stream inputStream = tridentTopology.newStream("inputStream", spout);

        Function splitFunction = new BaseFunction() {
            @Override
            public void execute(TridentTuple tuple, TridentCollector collector) {
                String sentence = tuple.getStringByField("sentence");
                if (!Strings.isNullOrEmpty(sentence)) {
                    String []words = sentence.split(" ");
                    for (String word : words) {
                        collector.emit(new Values(word, 1));
                    }
                }
            }
        };
        Stream wordStream = inputStream.each(new Fields("sentence"), splitFunction, new Fields("word", "count"));

        // 按照Word分组
        GroupedStream groupedStream = wordStream.groupBy(new Fields("word"));

        // 聚合算子
        CombinerAggregator<Integer> aggregator = new CombinerAggregator<Integer>() {
            @Override
            public Integer init(TridentTuple tuple) {
                return 1;
            }

            @Override
            public Integer combine(Integer val1, Integer val2) {
                return val1 + val2;
            }

            @Override
            public Integer zero() {
                return 0;
            }
        };

        // Redis Config
        JedisPoolConfig.Builder builder = new JedisPoolConfig.Builder();
        builder.setHost("localhost").setPort(6379);

        StateFactory factory = RedisMapState.opaque(builder.build());
        TridentState tridentState = groupedStream.persistentAggregate(factory, aggregator, new Fields("sum"))
                                                 .parallelismHint(5);

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("tridentTopology", conf, tridentTopology.build());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
