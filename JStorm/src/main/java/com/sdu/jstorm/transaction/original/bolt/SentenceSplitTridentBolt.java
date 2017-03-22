package com.sdu.jstorm.transaction.original.bolt;

import com.google.common.collect.Lists;
import com.sdu.jstorm.utils.Const;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.spout.TridentSpoutExecutor;
import org.apache.storm.trident.topology.BatchInfo;
import org.apache.storm.trident.topology.ITridentBatchBolt;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.topology.TridentBoltExecutor;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Trident Batch Bolt(Sentence Split). See{@link org.apache.storm.trident.spout.TridentSpoutExecutor}
 *
 * @author hanhan.zhang
 * */
public class SentenceSplitTridentBolt implements ITridentBatchBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(SentenceSplitTridentBolt.class);

    private Fields _field;

    private String _streamId;

    private boolean _direct;

    private String _separator;

    private BatchOutputCollector _collector;

    // consume task set(下游消费者Task集合)
    private AtomicInteger _consumeTaskIndex = new AtomicInteger(0);
    private List<Integer> _consumeTaskList;

    public SentenceSplitTridentBolt(Fields _field) {
        this(_field, Utils.DEFAULT_STREAM_ID, false);
    }

    public SentenceSplitTridentBolt(Fields _field, String _streamId, boolean _direct) {
        this._field = _field;
        this._streamId = _streamId;
        this._direct = _direct;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector) {
        this._collector = collector;
        this._separator = (String) conf.get(Const.SEPARATOR);

        // 下游消费者Task
        if (this._direct) {
            if (this._consumeTaskList == null) {
                this._consumeTaskList = Lists.newArrayList();
            }
            Map<String, Map<String, Grouping>> consumeTarget = context.getThisTargets();
            consumeTarget.forEach((streamId, target) ->
                    target.forEach((componentId, group) -> {
                        if (group.is_set_direct()) {
                            this._consumeTaskList.addAll(context.getComponentTasks(componentId));
                        }
                    })
            );
        }
    }

    // 事务处理逻辑
    @Override
    public void execute(BatchInfo batchInfo, Tuple tuple) {
        try {
            TransactionAttempt attempt = (TransactionAttempt) tuple.getValueByField(TridentSpoutExecutor.ID_FIELD);
            LOGGER.debug("开始处理事务拓扑[{}] ! ! !", attempt);
            String sentence = tuple.getStringByField("sentence");
            String []words = sentence.split(this._separator);
            for (String word : words) {
                if (this._direct) {
                    int curr = this._consumeTaskIndex.getAndIncrement() % this._consumeTaskList.size();
                    this._collector.emitDirect(curr, this._streamId, new Values(word, 1));
                } else {
                    this._collector.emit(this._streamId, new Values(word, 1));
                }
            }
        } catch (Exception e) {
            this._collector.reportError(e);
        }
    }

    // 每个事务处理完成后改方法被调用
    @Override
    public void finishBatch(BatchInfo batchInfo) {

    }

    // 每个事务处理之前该方法被调用
    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        TransactionAttempt attempt = (TransactionAttempt) batchId;
        LOGGER.debug("消息组{}的事务状态是:{}", batchGroup, attempt);
        return null;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this._streamId, this._direct, this._field);
        // 必须声明
        declarer.declareStream(TridentBoltExecutor.COORD_STREAM("fixed.split.bolt.stream.batch.group"), true, this._field);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
