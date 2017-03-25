package com.sdu.jstorm.monitor.hook;

import com.google.common.collect.Lists;
import com.sdu.jstorm.common.FixedCycleSpout;
import com.sdu.jstorm.common.operation.impl.CycleTupleGenerator;
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

import java.util.ArrayList;
import java.util.List;

/**
 * Storm工作节点和任务监控拓扑
 *
 * @author hanhan.zhang
 * */
public class JHookTopology {

    public static void main(String[] args) {
        // builder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // WorkerHook
        topologyBuilder.addWorkerHook(new JDefaultWorkerHook());

        // Spout
        JDataTranslator<JInputData<String>> translator = new JDataTranslator<JInputData<String>>() {
            @Override
            public String getCommitKey(JInputData<String> data) {
                return data.getKey();
            }

            @Override
            public List<JStreamTuple> apply(JInputData<String> data) {
                List<JStreamTuple> streamTuples = Lists.newLinkedList();
                String []words = data.getInputData().split(" ");
                for (String word : words) {
                    streams().forEach(stream -> {
                        JStreamTuple tuple = new JStreamTuple(new Values(word, 1), stream);
                        streamTuples.add(tuple);
                    });
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
        };

        JDataInputConfig<JInputData<String>> inputConfig = new JDataInputConfig<>();
        inputConfig.setMaxRetryTimes(5);
        inputConfig.setAutoCommit(false);
        inputConfig.setAutoCommitPeriodMs(1000);
        inputConfig.setDataSource(new JTestDataSource(JTestUtils.getData(1000)));
        inputConfig.setTranslator(translator);

        JDataInputSpout<JInputData<String>> inputSpout = new JDataInputSpout<>(inputConfig);

        // bolt
        JStreamPrintBolt printBolt = new JStreamPrintBolt();

        // 构建拓扑
        topologyBuilder.setSpout("inputSpout", inputSpout, 1);
        topologyBuilder.setBolt("printBolt", printBolt, 1)
                       .shuffleGrouping("inputSpout");


        Config config = new Config();
        // TaskHook
        config.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, Lists.newArrayList(JDefaultTaskHook.class.getName()));
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("hookTopology", config, topologyBuilder.createTopology());

    }

}
