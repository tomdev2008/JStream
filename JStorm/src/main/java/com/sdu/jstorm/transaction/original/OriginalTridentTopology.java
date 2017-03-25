package com.sdu.jstorm.transaction.original;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.jstorm.monitor.hook.JDefaultTaskHook;
import com.sdu.jstorm.transaction.original.bolt.SentenceSplitTridentBolt;
import com.sdu.jstorm.transaction.original.bolt.WordSumTridentBolt;
import com.sdu.jstorm.transaction.original.spout.FixedCycleTridentSpout;
import com.sdu.jstorm.utils.Const;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.*;
import org.apache.storm.trident.topology.BatchInfo;
import org.apache.storm.trident.topology.MasterBatchCoordinator;
import org.apache.storm.trident.topology.TridentBoltExecutor;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Trident Topology
 *
 * Note:
 *  1: streamName ---> streamBatchGroup
 *
 * @author hanhan.zhang
 * */
public class OriginalTridentTopology {

    public static void main(String[] args) {
        // builder
        TridentTopologyBuilder builder = new TridentTopologyBuilder();

        // spout
        Fields spoutFields = new Fields("sentence");
        ArrayList<List<Object>> sentenceTuple = Lists.newArrayList(new Values("the cow jumped over the moon"),
                                                                new Values("the man went to the store and bought some candy"),
                                                                new Values("four score and seven years ago"),
                                                                new Values("how many apples can you eat"));
        FixedCycleTridentSpout tridentSpout = new FixedCycleTridentSpout(spoutFields, sentenceTuple, 3);
        String spoutComponentName = "fixed.cycle.trident.spout";
        String spoutStreamName = "fixed.trident.spout.stream";
        // 事务组件特有,对应Zookeeper中路径(保存元数据)
        String spoutTxStateId = "fixed.spout.tx.state";
        String spoutBatchGroup = "fixed.spout.batch.stream.group";
        int spoutParallelism = 1;
        builder.setSpout(spoutComponentName, spoutStreamName, spoutTxStateId, tridentSpout, spoutParallelism, spoutBatchGroup);

        // bolt
        String splitComponentName = "fixed.split.trident.batch.bolt";
        String splitStreamName = "fixed.split.blot.stream";
        String splitBatchGroup = "fixed.split.bolt.stream.batch.group";
        int boltSplitParallelism = 1;

        // 表明哪些节点与该节点有联系
        Set<String> committerBatches = Sets.newHashSet();

        Map<String, String> batchGroups = Maps.newHashMap();
        batchGroups.put(splitStreamName, splitBatchGroup);

        SentenceSplitTridentBolt splitTridentBolt = new SentenceSplitTridentBolt(new Fields("word", "number"), splitStreamName, false);
        builder.setBolt(splitComponentName, splitTridentBolt, boltSplitParallelism, committerBatches, batchGroups)
                .shuffleGrouping(spoutComponentName, spoutStreamName);

        // sum bolt
        String sumComponentName = "fixed.sum.trident.batch.bolt";
        String sumStreamName = "fixed.sum.trident.bolt.stream";
        String sumBatchGroup = "fixed.sum.bolt.stream.batch.group";
        int boltSumParallelism = 1;

        // 表明哪些节点与该节点有联系
        Set<String> sumCommitterBatches = Sets.newHashSet();

        Map<String, String> sumBatchGroups = Maps.newHashMap();
        sumBatchGroups.put(sumStreamName, sumBatchGroup);

        WordSumTridentBolt sumTridentBolt = new WordSumTridentBolt();
        builder.setBolt(sumComponentName, sumTridentBolt, boltSumParallelism, sumCommitterBatches, sumBatchGroups)
                .fieldsGrouping(splitComponentName, splitStreamName, new Fields("word"));

        /**
         * {@link TridentTopologyBuilder#buildTopology()}
         * 1: {@link TridentSpoutCoordinator}负责接收的消息流
         *      -- {@link MasterBatchCoordinator#BATCH_STREAM_ID}
         *
         *      -- {@link MasterBatchCoordinator#SUCCESS_STREAM_ID}
         *
         *    {@link TridentBoltExecutor}负责接收的消息流:
         *      -- {@link TridentSpoutCoordinator}的{@link MasterBatchCoordinator#BATCH_STREAM_ID}
         *
         *      -- {@link TridentSpoutCoordinator}的{@link MasterBatchCoordinator#SUCCESS_STREAM_ID}
         *
         *      -- {@link FixedCycleTridentSpout}如果实现{@link ICommitterTridentSpout}, 则接收
         *         {@link TridentSpoutCoordinator}的{@link MasterBatchCoordinator#COMMIT_STREAM_ID}
         *
         * 2: 消息发送流程
         *    --> {@link MasterBatchCoordinator}发送信号{@link MasterBatchCoordinator#BATCH_STREAM_ID}
         *      --> {@link TridentBoltExecutor#execute(Tuple)}
         *        --> {@link TridentSpoutExecutor#execute(BatchInfo, Tuple)}
         *          --> {@link BatchSpoutExecutor#getEmitter(String, Map, TopologyContext)}
         *            --> {@link ITridentSpout#getEmitter(String, Map, TopologyContext)}
         *
         * 3: 消息成功消费
         *
         * */

        StormTopology stormTopology = builder.buildTopology();

        // config
        Config config = new Config();
        config.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, Lists.newArrayList(JDefaultTaskHook.class.getName()));
        config.put(Const.SEPARATOR, " ");
        config.setDebug(false);

        // 提交任务
        LocalCluster localCluster = new LocalCluster();
        String topologyName = "tridentTopology";
        try {
            localCluster.submitTopology(topologyName, config, stormTopology);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
