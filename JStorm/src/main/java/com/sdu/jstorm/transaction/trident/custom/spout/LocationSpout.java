package com.sdu.jstorm.transaction.trident.custom.spout;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Map;

/**
 * */
public class LocationSpout implements IBatchSpout {

    private boolean _cycle;

    private Fields _outputFields;

    private int _index;

    private int _maxBatchSize;

    private List<Object>[] _outputs;

    private Map<Long, List<List<Object>>> batchMap = Maps.newHashMap();

    public LocationSpout(int maxBatchSize, Fields fields, List<Object> ... outputs) {
        this(true, maxBatchSize, fields, outputs);
    }

    public LocationSpout(boolean cycle, int maxBatchSize, Fields fields, List<Object> ... outputs) {
        this._cycle = cycle;
        this._outputFields = fields;
        this._outputs = outputs;
        this._maxBatchSize = maxBatchSize;
    }

    @Override
    public void open(Map conf, TopologyContext context) {
        _index = 0;
    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> batchTuple = batchMap.get(batchId);
        if (batchTuple == null) {
            batchTuple = Lists.newArrayListWithCapacity(_maxBatchSize);
            if(_index >= _outputs.length && _cycle) {
                _index = 0;
            }
            for(int i = 0; _index < _outputs.length && i < _maxBatchSize; _index++, i++) {
                batchTuple.add(_outputs[_index]);
            }
            batchMap.put(batchId, batchTuple);
        }
        batchTuple.forEach(tuple -> collector.emit(tuple));
    }

    @Override
    public void ack(long batchId) {
        batchMap.remove(batchId);
    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return this._outputFields;
    }
}
