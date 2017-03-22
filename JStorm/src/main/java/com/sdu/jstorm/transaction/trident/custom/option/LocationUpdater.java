package com.sdu.jstorm.transaction.trident.custom.option;

import com.google.common.collect.Lists;
import com.sdu.jstorm.transaction.trident.custom.state.LocationDb;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Location State Updater
 *
 * @author hanhan.zhang
 * */
public class LocationUpdater extends BaseStateUpdater<LocationDb> {

    @Override
    public void updateState(LocationDb state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Long> userIds = Lists.newArrayListWithCapacity(tuples.size());
        List<String> locations = Lists.newArrayListWithExpectedSize(tuples.size());
        tuples.forEach(tuple -> {
            userIds.add(tuple.getLong(0));
            locations.add(tuple.getString(1));
        });
        state.setBulkLocation(userIds, locations);
    }
}
