package com.sdu.jstorm.group.router;

import com.google.common.collect.Lists;
import com.sdu.jstorm.data.JDataInputConfig;
import com.sdu.jstorm.data.JDataInputSpout;
import com.sdu.jstorm.test.JInputData;
import com.sdu.jstorm.test.JStreamPrintBolt;
import com.sdu.jstorm.test.JTestDataSource;
import com.sdu.jstorm.translator.JDataTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import com.sdu.jstorm.utils.JTestUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * 按照StreamName订阅
 *
 * @author hanhan.zhang
 * */
public class JStreamRouteTopology {

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        JDataTranslator<JInputData<String>> translator = new JDataTranslator<JInputData<String>>() {

            private List<String> streams = Lists.newArrayList("dataWordStream", "dataWordCountStream");

            @Override
            public String getCommitKey(JInputData<String> data) {
                return data.getKey();
            }

            @Override
            public List<JStreamTuple> apply(JInputData<String> data) {
                List<JStreamTuple> streamTuples = Lists.newLinkedList();
                streams.forEach(stream -> {
                    String inputData = data.getInputData();
                    String []words = inputData.split(" ");
                    for (String word : words) {
                        if (stream.equals("dataWordStream")) {
                            JStreamTuple tuple = new JStreamTuple(new Values(word), stream);
                            streamTuples.add(tuple);
                        } else {
                            JStreamTuple tuple = new JStreamTuple(new Values(word, 1), stream);
                            streamTuples.add(tuple);
                        }
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
                if (stream.equals("dataWordStream")) {
                    return new Fields("word");
                } else if (stream.equals("dataWordCountStream")) {
                    return new Fields("word", "count");
                }
                return null;
            }

            @Override
            public List<String> streams() {
                return streams;
            }
        };

        JDataInputConfig<JInputData<String>> inputConfig = new JDataInputConfig<>();
        inputConfig.setDataSource(new JTestDataSource(JTestUtils.getData(1000)));
        inputConfig.setTranslator(translator);
        inputConfig.setAutoCommitPeriodMs(1000);
        inputConfig.setAutoCommit(false);
        inputConfig.setMaxRetryTimes(5);
        JDataInputSpout<JInputData<String>> inputSpout = new JDataInputSpout<>(inputConfig);

        JStreamPrintBolt wordPrintBolt = new JStreamPrintBolt();
        JStreamPrintBolt wordCountPrintBolt = new JStreamPrintBolt();

        // 构建拓扑[wordPrintBolt订阅'dataWordStream'流, wordCountPrintBolt订阅'dataWordCountStream'流]
        topologyBuilder.setSpout("dataInputSpout", inputSpout, 1);
        topologyBuilder.setBolt("wordPrintBolt", wordPrintBolt, 1)
                       .shuffleGrouping("dataInputSpout", "dataWordStream");
        topologyBuilder.setBolt("wordCountPrintBolt", wordCountPrintBolt, 1)
                       .shuffleGrouping("dataInputSpout", "dataWordCountStream");

        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
        config.setNumAckers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("streamRouterTopology", config, topologyBuilder.createTopology());
    }

}
