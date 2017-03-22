package com.sdu.jstorm.transaction.trident.custom.option;

import com.sdu.jstorm.transaction.trident.custom.state.LocationDb;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Location Query
 *
 * @author hanhan.zhang
 * */
public class LocationQuery extends BaseQueryFunction<LocationDb, String> {

    @Override
    public List<String> batchRetrieve(LocationDb state, List<TridentTuple> tuples) {
        List<Long> userIds = tuples.stream()
                                    .map(tuple -> tuple.getLong(0))
                                    .collect(Collectors.toList());
        return state.getBulkLocation(userIds);
    }

    @Override
    public void execute(TridentTuple tuple, String result, TridentCollector collector) {
        collector.emit(new Values(result));
    }
}
