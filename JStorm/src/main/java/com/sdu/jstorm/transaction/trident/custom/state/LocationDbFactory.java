package com.sdu.jstorm.transaction.trident.custom.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Location State Factory
 *
 * @author hanhan.zhang
 * */
public class LocationDbFactory implements StateFactory {

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return new LocationDb();
    }

}
