package com.sdu.jstorm.group.direct;

import com.google.common.collect.Lists;
import com.sdu.jstorm.data.JDataInputConfig;
import com.sdu.jstorm.data.JDataInputSpout;
import com.sdu.jstorm.test.JInputData;
import com.sdu.jstorm.test.JStreamPrintBolt;
import com.sdu.jstorm.test.JTestDataSource;
import com.sdu.jstorm.translator.JDataTranslator;
import com.sdu.jstorm.translator.JStreamTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import com.sdu.jstorm.utils.JTestUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class JDirectGroupTopology {

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // Spout
        JDataTranslator<JInputData<String>> translator = new JDataTranslator<JInputData<String>>() {

            private String streamName = "dataStream";

            @Override
            public String getCommitKey(JInputData<String> data) {
                return data.getKey();
            }

            @Override
            public List<JStreamTuple> apply(JInputData<String> inputData) {
                List<JStreamTuple> streamTuples = Lists.newLinkedList();
                String[] words = inputData.getInputData().split(" ");
                for (String word : words) {
                    Values tuple = new Values(word, 1);
                    streamTuples.add(new JStreamTuple(tuple, streamName));
                }
                return streamTuples;
            }

            @Override
            public boolean isDirectFor(String stream) {
                return false;
            }

            @Override
            public Fields getFieldsFor(String stream) {
                return new Fields("word", "count");
            }

            @Override
            public List<String> streams() {
                return Lists.newArrayList(streamName);
            }
        };
        JDataInputConfig<JInputData<String>> inputConfig = new JDataInputConfig<>();
        inputConfig.setAutoCommit(false);
        inputConfig.setMaxRetryTimes(5);
        inputConfig.setAutoCommitPeriodMs(1000);
        inputConfig.setDataSource(new JTestDataSource(JTestUtils.getData(1000)));
        inputConfig.setTranslator(translator);
        JDataInputSpout<JInputData<String>> inputSpout = new JDataInputSpout<>(inputConfig);

        // Direct Bolt
        JStreamTranslator streamTranslator = new JStreamTranslator() {

            private String streamName = "directStream";

            @Override
            public List<JStreamTuple> apply(Tuple tuple) {
                String word = tuple.getStringByField("word");
                int count = tuple.getIntegerByField("count");
                return Lists.newArrayList(new JStreamTuple(new Values(word, count), streamName));
            }

            @Override
            public Fields getFieldsFor(String stream) {
                return new Fields("word", "count");
            }

            @Override
            public List<String> streams() {
                return Lists.newArrayList(streamName);
            }
        };
        JStreamDirectBolt directBolt = new JStreamDirectBolt(streamTranslator);

        // 输出Bolt
        JStreamPrintBolt printBolt = new JStreamPrintBolt();

        // 构建拓扑
        topologyBuilder.setSpout("dataSpout", inputSpout, 1);
        topologyBuilder.setBolt("directBolt", directBolt, 1)
                       .shuffleGrouping("dataSpout", "dataStream");
        topologyBuilder.setBolt("printBolt", printBolt, 1)
                       .directGrouping("directBolt", "directStream");

        //
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(1);
        config.setNumWorkers(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("directTopology", config, topologyBuilder.createTopology());


    }

}
