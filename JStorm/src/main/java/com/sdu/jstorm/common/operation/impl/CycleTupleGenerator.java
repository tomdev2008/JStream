package com.sdu.jstorm.common.operation.impl;

import com.google.common.collect.Lists;
import com.sdu.jstorm.common.operation.TupleGenerator;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cycle Data Source
 *
 * @author hanhan.zhang
 * */
public class CycleTupleGenerator implements TupleGenerator {

    private static final ArrayList<List<Object>> DEFAULT_DATA_SOURCE = Lists.newArrayList(new Values("the cow jumped over the moon"),
                                                                                            new Values("the man went to the store and bought some candy"),
                                                                                            new Values("four score and seven years ago"),
                                                                                            new Values("how many apples can you eat"));

    private ArrayList<List<Object>> _dataSource;

    private AtomicInteger _index = new AtomicInteger(0);

    public CycleTupleGenerator() {
        this(DEFAULT_DATA_SOURCE);
    }

    public CycleTupleGenerator(ArrayList<List<Object>> dataSource) {
        this._dataSource = dataSource;
    }

    @Override
    public List<Object> generator() {
        if (this._index.get() >= this._dataSource.size()) {
            this._index.set(0);
        }
        return this._dataSource.get(this._index.getAndIncrement());
    }
}
