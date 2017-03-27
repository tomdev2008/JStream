package com.sdu.jstorm.aggregate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.jstorm.kafka.JKafkaSpout;
import com.sdu.jstorm.kafka.JKafkaSpoutConfig;
import com.sdu.jstorm.test.JStreamPrintBolt;
import com.sdu.jstorm.translator.JKafkaTranslator;
import com.sdu.jstorm.translator.JWindowTranslator;
import com.sdu.jstorm.tuple.JStreamTuple;
import com.sdu.jstorm.utils.JGsonUtils;
import com.sdu.jstorm.utils.JKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Storm滑动窗口(如：每隔5秒计算过去20秒的用户的浏览量)
 *
 * Note：
 *
 *  +------------------------------------------------------------------------------------------------------+
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
    +------------------------------------------------------------------------------------------------------+

 ， * @author hanhan.zhang
 * */
public class JWindowTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(JWindowTopology.class);

    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        JKafkaTranslator<String, String> translator = new JKafkaTranslator<String, String>() {

            private String streamName = "kafkaStream";

            @Override
            public JStreamTuple apply(ConsumerRecord<String, String> record) {
                String value = record.value();
                JKafkaMessage msg = JGsonUtils.fromJson(value, JKafkaMessage.class);
                if (msg != null) {
                    return new JStreamTuple(new Values(msg.getUserId(), msg.getAction(), msg.getTimestamp()), streamName);
                }
                return null;
            }

            @Override
            public Fields getFieldsFor(String stream) {
                if (stream.equals(streamName)) {
                    return new Fields("userId", "action", "timestamp");
                }
                return null;
            }

            @Override
            public List<String> streams() {
                return Lists.newArrayList(streamName);
            }
        };
        // kafka spout
        Map<String, Object> kafkaProps = Maps.newHashMap();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        JKafkaSpoutConfig<String, String> spoutConfig = new JKafkaSpoutConfig<>();
        spoutConfig.setAutoCommit(false);
        spoutConfig.setGroupId("JStorm_Kafka_Message_Group");
        spoutConfig.setTopics(Collections.singletonList("JK_Message"));
        spoutConfig.setTranslator(translator);
        spoutConfig.setKafkaProps(kafkaProps);

        JKafkaSpout<String, String> kafkaSpout = new JKafkaSpout<>(spoutConfig);

        // WindowBolt[统计五秒内用户行为]
        JWindowTranslator windowTranslator = new JWindowTranslator() {

            private List<String> streams = Lists.newArrayList("windowStream");

            @Override
            public List<JStreamTuple> apply(TupleWindow tupleWindow) {
                List<Tuple> newTuple = tupleWindow.getNew();
                Map<String, Map<String, Integer>> userActions = Maps.newHashMap();
                newTuple.forEach(tuple -> {
                    String userId = tuple.getStringByField("userId");
                    String action = tuple.getStringByField("action");
                    Map<String, Integer> actions = userActions.get(userId);
                    if (actions == null) {
                        actions = Maps.newHashMapWithExpectedSize(3);
                        actions.put(action, 1);
                        userActions.put(userId, actions);
                    } else {
                        Integer count = actions.get(action);
                        if (count == null) {
                            actions.put(action, 1);
                        } else {
                            actions.put(action, count + 1);
                        }
                    }
                });

                List<JStreamTuple> streamTuples = Lists.newLinkedList();

                streams.forEach(stream ->
                    userActions.forEach((userId, actions) ->
                        actions.forEach((action, count) -> {
                            Values values = new Values(userId, action, count);
                            streamTuples.add(new JStreamTuple(values, stream));
                        })
                    )
                );

                return streamTuples;
            }

            @Override
            public Fields getFieldsFor(String stream) {
                return new Fields("userId", "action", "count");
            }

            @Override
            public List<String> streams() {
                return streams;
            }
        };
        JStreamWindowBolt windowBolt = new JStreamWindowBolt(windowTranslator);
        windowBolt.withWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS));

        // 输出流
        JStreamPrintBolt printBolt = new JStreamPrintBolt();

        // 构建拓扑
        topologyBuilder.setSpout("kafkaSpout", kafkaSpout, 1);
        topologyBuilder.setBolt("windowBolt", windowBolt, 2)
                       .shuffleGrouping("kafkaSpout", "kafkaStream");
        topologyBuilder.setBolt("printBolt", printBolt, 1)
                       .shuffleGrouping("windowBolt", "windowStream");

        // 设置超时时间及Spout.pending
        Config config = new Config();
        config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 5);   // 1秒一条消息, 窗口长度为5


        String topologyName = "windowTopology";
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(topologyName, config, topologyBuilder.createTopology());
    }

}
