package com.sdu.jstorm.trident;

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
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.state.StateFactory;
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
        JDataTranslator<JInputData<String>> translator = new JDataTranslator<JInputData<String>>() {
            @Override
            public String getCommitKey(JInputData<String> data) {
                return data.getKey();
            }

            @Override
            public List<JStreamTuple> apply(JInputData<String> data) {
                List<JStreamTuple> streamTuples = Lists.newLinkedList();
                streams().forEach(stream -> {
                    String []words = data.getInputData().split(" ");
                    for (String word : words) {
                        streamTuples.add(new JStreamTuple(new Values(word), stream));
                    }
                });
                return streamTuples;
            }

            @Override
            public boolean isDirectFor(String stream) {
                return false;
            }

            @Override
            public Fields getFieldsFor(String stream) {
                return new Fields("word");
            }
        };

        JDataInputConfig<JInputData<String>> inputConfig = new JDataInputConfig<>();
        inputConfig.setMaxRetryTimes(5);
        inputConfig.setAutoCommit(false);
        inputConfig.setAutoCommitPeriodMs(1000);
        inputConfig.setDataSource(new JTestDataSource(JTestUtils.getData(1000)));
        inputConfig.setTranslator(translator);

        JDataInputSpout<JInputData<String>> inputSpout = new JDataInputSpout<>(inputConfig);

        Stream inputStream = tridentTopology.newStream("inputStream", inputSpout);

        // 按照Word分组
        GroupedStream groupedStream = inputStream.groupBy(new Fields("word"));

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
        builder.setHost("").setPort(0);

        StateFactory factory = new RedisState.Factory(builder.build());
        TridentState tridentState = groupedStream.persistentAggregate(factory, aggregator, new Fields("word", "count"))
                                                 .parallelismHint(5);

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tridentTopology", conf, tridentTopology.build());
    }

}
