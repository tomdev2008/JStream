package com.sdu.jstorm.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.jstorm.kafka.JKafkaSpout;
import com.sdu.jstorm.kafka.JKafkaSpoutConfig;
import com.sdu.jstorm.tuple.JStreamTuple;
import com.sdu.jstorm.translator.JKafkaTranslator;
import com.sdu.jstorm.test.JStreamPrintBolt;
import com.sdu.jstorm.utils.JGsonUtils;
import com.sdu.jstorm.utils.JKafkaMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

        JKafkaTranslator<String, String> translator = new JKafkaTranslator<String, String>() {

            private String streamName = "kafkaStream";

            @Override
            public JStreamTuple apply(ConsumerRecord<String, String> record) {
                String value = record.value();
                if (Strings.isNotEmpty(value)) {
                    JKafkaMessage msg = JGsonUtils.fromJson(value, JKafkaMessage.class);
                    if (msg != null) {
                        return new JStreamTuple(new Values(msg.getUserId(), msg.getAction(), msg.getTimestamp()), streamName);
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
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        JKafkaSpoutConfig<String, String> spoutConfig = new JKafkaSpoutConfig<>();
        spoutConfig.setAutoCommit(false);
        spoutConfig.setGroupId("JStorm_Kafka_Message_Group");
        spoutConfig.setTopics(Collections.singletonList("JK_Message"));
        spoutConfig.setTranslator(translator);
        spoutConfig.setKafkaProps(kafkaProps);

        JKafkaSpout<String, String> kafkaSpout = new JKafkaSpout<>(spoutConfig);
        JStreamPrintBolt printBolt = new JStreamPrintBolt();

        // 构建拓扑
        topologyBuilder.setSpout("kafkaSpout", kafkaSpout, 1);
        topologyBuilder.setBolt("kafkaPrintBolt", printBolt, 2)
                       .shuffleGrouping("kafkaSpout", "kafkaStream");

        Config config = new Config();
        config.setNumWorkers(1);
        // Worker与Ack最好为一一对应
        config.setNumAckers(1);
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("scheduleTopology", config, topologyBuilder.createTopology());
    }

}
