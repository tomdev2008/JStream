package com.sdu.jstorm.transaction.original.spout;

import com.google.common.collect.Maps;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.trident.topology.TridentTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trident Spout
 *
 * @author hanhan.zhang
 * */
public class FixedCycleTridentSpout implements ITridentSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixedCycleTridentSpout.class);

    private Fields _fields;

    private ArrayList<List<Object>> _outputTuple;

    // 批量消息大小
    private int _batchSize;

    public FixedCycleTridentSpout(Fields _fields, ArrayList<List<Object>> _outputTuple, int batchSize) {
        this._fields = _fields;
        this._outputTuple = _outputTuple;
        this._batchSize = batchSize;
    }

    @Override
    public BatchCoordinator getCoordinator(String txStateId, Map conf, TopologyContext context) {
        int taskSize = context.getComponentTasks(TridentTopologyBuilder.spoutIdFromCoordinatorId(context.getThisComponentId())).size();
        return new FixedCycleTridentCoordinator(taskSize);
    }

    @Override
    public Emitter getEmitter(String txStateId, Map conf, TopologyContext context) {
        return new FixedCycleTridentEmitter(context.getThisTaskIndex());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return _fields;
    }

    /**
     * 协调Spout数据发送
     *
     * Note ：
     *  Tuple数据结构：Map<Integer, List<List<Object>>>
     *
     * @author hanhan.zhang
     * */
    protected class FixedCycleTridentCoordinator implements BatchCoordinator<Map<Integer, List<List<Object>>>> {

        // tuple partition = executor number
        private int _numPartitions;

        // key = 事务ID, _value = 事务ID对应的起始索引(事务失败,可重新发数据)
        private Map<Long, Integer> _txIndices = new HashMap();

        // 数据发送索引位置
        private int _emittedIndex = 0;

        private long _commitTxid;

        public FixedCycleTridentCoordinator(int _numPartitions) {
            this._numPartitions = _numPartitions;
        }

        public Map<Integer, List<List<Object>>> initializeTransaction(long txid, Map<Integer, List<List<Object>>> prevMetadata, Map<Integer, List<List<Object>>> currMetadata) {
            if (currMetadata != null) {
                return currMetadata;
            }
            // 记录事务ID对应的发送消息的起始位置所以(保障事务ID失败,消息重新发送)
            if (this._txIndices.containsKey(txid)) {
                LOGGER.warn("编号[{}]的事务数据处理失败,重新发送数据 !", txid);
                int start = this._txIndices.get(txid);
                return getSendTuple(start, this._numPartitions, _batchSize);
            }
            LOGGER.debug("编号[{}]的事务数据索引位置是[{}]", txid, this._emittedIndex);
            this._txIndices.put(txid, this._emittedIndex);
            Map<Integer, List<List<Object>>> partitionTupleMap = this.getSendTuple(this._emittedIndex, this._numPartitions, _batchSize);
            this._emittedIndex += this._numPartitions * _batchSize;
            return partitionTupleMap;
        }

        @Override
        public void success(long txid) {
            LOGGER.debug("编号[{}]的事务数据处理成功 !", txid);
            this._txIndices.remove(txid);
            this._commitTxid = txid;
        }

        public boolean isReady(long txid) {
            return txid > this._commitTxid;
        }

        // 释放资源
        public void close() {
            this._txIndices.clear();
            this._txIndices = null;
        }

        protected Map<Integer, List<List<Object>>> getSendTuple(int index, int partition, int batchSize) {
            // 按照下游消费者Task数目分区,每个Task接收_batchSize消息
            Map<Integer, List<List<Object>>> partitionTupleMap = Maps.newHashMap();
            for (int i = 0; i < partition; ++i) {
                List<List<Object>> tupleList = new ArrayList<>(batchSize);
                for (int j = index; j < batchSize; ++j) {
                    tupleList.add(_outputTuple.get(j % _outputTuple.size()));
                }
                partitionTupleMap.put(i, tupleList);
            }
            return partitionTupleMap;
        }
    }

    /**
     * 实现Tuple数据发送
     *
     * @author hanhan.zhang
     * */
    protected class FixedCycleTridentEmitter implements Emitter<Map<Integer, List<List<Object>>>> {

        // executor index
        private int _index;

        public FixedCycleTridentEmitter(int _index) {
            this._index = _index;
        }

        @Override
        public void emitBatch(TransactionAttempt tx, Map<Integer, List<List<Object>>> coordinatorMeta, TridentCollector collector) {
            List<List<Object>> tupleList = coordinatorMeta.get(this._index);
            tupleList.forEach(tuple -> collector.emit(tuple));
        }

        @Override
        public void success(TransactionAttempt tx) {

        }

        @Override
        public void close() {

        }
    }
}
