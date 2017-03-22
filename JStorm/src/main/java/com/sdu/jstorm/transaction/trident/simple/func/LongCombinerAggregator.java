package com.sdu.jstorm.transaction.trident.simple.func;

import com.google.common.base.Strings;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * Long Aggregator
 *
 * @author hanhan.zhang
 * */
public class LongCombinerAggregator implements CombinerAggregator<Long> {

    protected String fieldName;

    public LongCombinerAggregator() {

    }

    public LongCombinerAggregator(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Long init(TridentTuple tuple) {
        if (Strings.isNullOrEmpty(fieldName)) {
            return 1L;
        }
        return tuple.getLongByField(fieldName);
    }

    @Override
    public Long combine(Long val1, Long val2) {
        return val1 + val2;
    }

    @Override
    public Long zero() {
        return 0L;
    }
}
