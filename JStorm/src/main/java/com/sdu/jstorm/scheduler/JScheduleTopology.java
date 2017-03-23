package com.sdu.jstorm.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.jstorm.kafka.JKafkaSpout;
import com.sdu.jstorm.kafka.JKafkaSpoutConfig;
import com.sdu.jstorm.kafka.JKafkaTuple;
import com.sdu.jstorm.kafka.JTranslator;
import com.sdu.jstorm.test.JKafkaPrintBolt;
import com.sdu.jstorm.utils.GsonUtils;
import com.sdu.jstorm.utils.JKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.util.Strings;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.List;
import java.util.Map;

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

        JTranslator<String, String> translator = new JTranslator<String, String>() {

            private String streamName = "kafkaStream";

            @Override
            public JKafkaTuple apply(ConsumerRecord<String, String> record) {
                String value = record.value();
                if (Strings.isNotEmpty(value)) {
                    JKafkaMessage msg = GsonUtils.fromJson(value, JKafkaMessage.class);
                    if (msg != null) {
                        return new JKafkaTuple(new Values(msg.getUserId(), msg.getAction(), msg.getTimestamp()), streamName);
                    }
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

        JKafkaSpoutConfig<String, String> spoutConfig = new JKafkaSpoutConfig<>();
        spoutConfig.setAutoCommit(false);
        spoutConfig.setGroupId("JStorm_Message_Test_Group");
        spoutConfig.setTopics(Collections.singletonList("JK_Message"));
        spoutConfig.setKeyDeserializer(new StringDeserializer());
        spoutConfig.setValueDeserializer(new StringDeserializer());
        spoutConfig.setTranslator(translator);
        spoutConfig.setKafkaProps(kafkaProps);

        JKafkaSpout<String, String> kafkaSpout = new JKafkaSpout<>(spoutConfig);
        JKafkaPrintBolt printBolt = new JKafkaPrintBolt();

        // 构建拓扑
        topologyBuilder.setSpout("kafkaSpout", kafkaSpout, 1);
        topologyBuilder.setBolt("kafkaPrintBolt", printBolt, 1)
                       .shuffleGrouping("kafkaSpout");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("scheduleTopology", config, topologyBuilder.createTopology());

    }

}
