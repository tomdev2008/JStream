package com.sdu.jstorm.topology.window;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.jstorm.common.FixedCycleSpout;
import com.sdu.jstorm.common.operation.TupleGenerator;
import com.sdu.jstorm.common.operation.TupleOperation;
import com.sdu.jstorm.topology.window.bolt.UserActionBolt;
import com.sdu.jstorm.topology.window.bolt.UserActionStatisticBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.WindowedBoltExecutor;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Storm滑动窗口(如：每隔5秒计算过去20秒的用户的浏览量)
 *
 * Note：
 *
 *  |------------------------------------------------------------------------------------------------------|
    |1：Storm Window Tuple                                                                                 |
    | 1' 基于Tuple Size                                                                                     |
    |    a：WindowLength + SlideLength <= {@link Config#TOPOLOGY_MAX_SPOUT_PENDING}                        |
    | 2' 基于Tuple Timestamp                                                                                |
    |    a：WindowLength + SlideLength <= {@link Config#TOPOLOGY_MESSAGE_TIMEOUT_SECS}                     |
    |    b: declare 'timestamp' field name                                                                 |
    | 3' Tuple Size和Tuple Timestamp混合使用                                                                 |
    |                                                                                                      |
    |2：{@link WindowedBoltExecutor}                                                                       |
    | 1' {@link TriggerPolicy}负责触发窗口的滑动                                                              |
    | 2' {@link EvictionPolicy}负责触发窗口的生成                                                             |
    | 3' {@link WindowLifecycleListener}负责Tuple处理(失效Event确认Tuple,处理)                                 |
    | 4' {@link WindowManager}管理窗口(窗口滑动、窗口生成、窗口事件处理)                                           |
    |------------------------------------------------------------------------------------------------------|

 ， * @author hanhan.zhang
 * */
public class SlideWindowTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(SlideWindowTopology.class);

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // spout
        TupleGenerator tupleGenerator = new TupleGenerator() {

            Random random = new Random();

            ArrayList<String> actionList = Lists.newArrayList("浏览", "点击", "下单");

            @Override
            public List<Object> generator() {
                return new Values(UUID.randomUUID().toString(),
                                    actionList.get(random.nextInt(actionList.size() - 1)),
                                    System.currentTimeMillis());
            }
        };

        String spoutComponentName = "cycle.spout";
        Fields spoutField = new Fields("uuid", "actionType", "actionTime");
        String spoutStream = "fixed.cycle.spout.stream";
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStream, false, spoutField, tupleGenerator);
        topologyBuilder.setSpout(spoutComponentName, cycleSpout, 1);

        // window bolt
        String actionComponentName = "user.action.bolt";
        String actionStream = "user.action.window.stream";
        Fields actionField = new Fields("actionType", "actionTime", "number");
        UserActionBolt userActionBolt = new UserActionBolt(actionStream, false, actionField);
        topologyBuilder.setBolt(actionComponentName, userActionBolt, 1)
                       .shuffleGrouping(spoutComponentName, spoutStream);

        // window bolt
        TupleOperation<TupleWindow> operation = (collector, windowTuple) -> {
            List<Tuple> total = windowTuple.get();
            Map<String, Integer> sumMap = Maps.newHashMap();
            total.forEach(tuple -> {
                String actionType = tuple.getStringByField("actionType");
                int num = tuple.getIntegerByField("number");
                Integer oldNumber = sumMap.get(actionType);
                if (oldNumber == null) {
                    oldNumber = 0;
                }
                oldNumber += num;
                sumMap.put(actionType, oldNumber);
            });
            sumMap.forEach((actionType, num) -> LOGGER.info("在过去20MS内，{} Action数据是：{}", actionType, num));
        };

        String statisticComponentName = "user.action.statistic.bolt";
//        BaseWindowedBolt.Count windowLength = new BaseWindowedBolt.Count(10);
//        BaseWindowedBolt.Count slideLength = new BaseWindowedBolt.Count(10);
        BaseWindowedBolt.Duration windowLength = new BaseWindowedBolt.Duration(20, TimeUnit.MILLISECONDS);
        BaseWindowedBolt.Duration slideLength = new BaseWindowedBolt.Duration(10, TimeUnit.MILLISECONDS);
        UserActionStatisticBolt statisticBolt = (UserActionStatisticBolt) new UserActionStatisticBolt(operation)
                                                        .withWindow(windowLength, slideLength);
        statisticBolt.withTimestampField("actionTime");

        topologyBuilder.setBolt(statisticComponentName, statisticBolt, 1)
                       .fieldsGrouping(actionComponentName, actionStream, new Fields("actionType"));

        // 提交作业
        Config config = new Config();

        String topologyName = "windowTopology";
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
    }

}
