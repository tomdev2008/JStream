package com.sdu.jstorm.scheduler;

import com.sdu.jstorm.common.FixedCycleSpout;
import com.sdu.jstorm.common.operation.impl.CycleTupleGenerator;
import com.sdu.jstorm.topology.group.bolt.SentenceSplitBolt;
import com.sdu.jstorm.topology.group.bolt.WordSumBolt;
import com.sdu.jstorm.utils.Const;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Storm调度拓扑
 *
 * @author hanhan.zhang
 * */
public class JScheduleTopology {

    public static void main(String[] args) {

        // config storm.yaml
        System.setProperty("storm.conf.file", "storm/storm.yaml");

//        Utils.readStormConfig();


        // builder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // spout
        String spoutStreamId = "topology.flow.cycle.spout.stream";
        String spoutComponentName = "sentence.cycle.spout";
        boolean spoutStreamDirect = true;
        int spoutExecutorParallelism = 1;
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStreamId, spoutStreamDirect, new Fields("sentence"), new CycleTupleGenerator());
        topologyBuilder.setSpout (spoutComponentName, cycleSpout, spoutExecutorParallelism);

        // bolt
        String splitStreamId = "topology.flow.split.bolt.stream";
        String splitComponentName = "sentence.split.bolt";
        boolean splitStreamDirect = false;
        int splitExecutorParallelism = 2;
        // 默认:executor = task
        int splitBoltTask = 4;
        SentenceSplitBolt splitBolt = new SentenceSplitBolt(splitStreamId, splitStreamDirect);
        topologyBuilder.setBolt(splitComponentName, splitBolt, splitExecutorParallelism)
                        .setNumTasks(splitBoltTask)
                        .directGrouping(spoutComponentName, spoutStreamId);

        String sumComponentName = "word.sum.bolt";
        int sumExecutorParallelism = 2;
        WordSumBolt sumBolt = new WordSumBolt();
        topologyBuilder.setBolt(sumComponentName, sumBolt, sumExecutorParallelism)
                        .fieldsGrouping(splitComponentName, splitStreamId, new Fields("word"));

        Config config = new Config();
        config.setDebug(false);
        config.put(Const.SEPARATOR, " ");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("flowTopology", config, topologyBuilder.createTopology());

    }

}
