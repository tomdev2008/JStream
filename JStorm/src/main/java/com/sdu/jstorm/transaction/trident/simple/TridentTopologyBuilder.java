package com.sdu.jstorm.transaction.trident.simple;

import com.google.common.base.Strings;
import com.sdu.jstorm.transaction.trident.simple.func.LongCombinerAggregator;
import com.sdu.jstorm.transaction.trident.simple.state.Option;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.fluent.GroupedStream;
import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;



/**
 * @author hanhan.zhang
 * */
public class TridentTopologyBuilder {

    private static final Option<String> option = new Option<>();

    static {
        option.setLocalCacheSize(10);
        option.setStateType(StateType.TRANSACTIONAL);
    }

    public static void main(String []args) {
        // storm spout
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);


        TridentTopology tridentTopology = new TridentTopology();
        // zookeeper create 'sentence.spout' node to keep metadata
        Stream sentenceStream = tridentTopology.newStream("sentence.spout", spout);
        // sentence split to word tuple
        Stream wordStream = sentenceStream.each(new Fields("sentence"),
                new Function() {
                    @Override
                    public void execute(TridentTuple tuple, TridentCollector collector) {
                        String sentence = tuple.getStringByField("sentence");
                        if (!Strings.isNullOrEmpty(sentence)) {
                            String []words = sentence.split(" ");
                            for (String word : words) {
                                System.out.println("send word [" + word + "]");
                                collector.emit(new Values(word));
                            }
                        }
                    }

                    @Override
                    public void prepare(Map conf, TridentOperationContext context) {

                    }

                    @Override
                    public void cleanup() {

                    }
                },
                new Fields("word"));
        // group by word
        GroupedStream groupStream = wordStream.groupBy(new Fields("word"));
        // aggregate word and persistent
        TridentState tridentState = groupStream.persistentAggregate(new MemoryMapState.Factory() /*RedisMapState.build(option)*/,
                new Fields("word"), new LongCombinerAggregator(),
                new Fields("count"));
        // parallelism
        tridentState = tridentState.parallelismHint(6);

        StormTopology stormTopology = tridentTopology.build();

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tridentTopology", conf, stormTopology);
    }
}
