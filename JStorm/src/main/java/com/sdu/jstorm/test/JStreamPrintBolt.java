package com.sdu.jstorm.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.jstorm.utils.JGsonUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class JStreamPrintBolt extends BaseBasicBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(JStreamPrintBolt.class);

    private String componentName;
    private int taskId;

    private JPrintTransfer printTransfer = new JDefaultPrintTransfer();

    public JStreamPrintBolt() {

    }

    public JStreamPrintBolt(JPrintTransfer printTransfer) {
        this.printTransfer = printTransfer;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        componentName = context.getThisComponentId();
        taskId = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Object output = this.printTransfer.transfer(input);
        LOGGER.info("Tuple Output : {}", JGsonUtils.formatJson(output));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private class JDefaultPrintTransfer implements JPrintTransfer {
        @Override
        public Output transfer(Tuple tuple) {
            Output output = new Output();

            // Ack锚定信息
            MessageId messageId = tuple.getMessageId();
            Map<Long, Long> anchorsToIds = messageId.getAnchorsToIds();
            messageId.getAnchors().forEach(ackInputMsgId -> {
                Ack ack = new Ack(ackInputMsgId, anchorsToIds.get(ackInputMsgId));
                output.addAckMsg(ack);
            });

            // 消息来源组件信息
            output.setFromComponent(tuple.getSourceComponent());
            output.setFromStream(tuple.getSourceStreamId());
            output.setFromTask(tuple.getSourceTask());

            // 消息分发组件信息
            output.setToComponent(componentName);
            output.setToTask(taskId);

            // 消息信息
            List<String> fields = tuple.getFields().toList();
            if (fields != null) {
                fields.forEach(field -> {
                    Object data = tuple.getValueByField(field);
                    output.addTupleData(field, data);
                });
            }

            return output;
        }
    }

    @Setter
    @Getter
    private class Output {
        private List<Ack> ackMsg;
        private String fromComponent;
        private String fromStream;
        private int fromTask;
        private String toComponent;
        private int toTask;
        private Map<String, Object> tupleData;

        Output() {
            ackMsg = Lists.newLinkedList();
            tupleData = Maps.newHashMap();
        }

        void addTupleData(String key, Object data) {
            tupleData.put(key, data);
        }

        void addAckMsg(Ack ack) {
            ackMsg.add(ack);
        }
    }

    @Setter
    @Getter
    @AllArgsConstructor
    private class Ack {
        private long ackInputMsgId;
        private long ackOutputMsgId;
    }
}
