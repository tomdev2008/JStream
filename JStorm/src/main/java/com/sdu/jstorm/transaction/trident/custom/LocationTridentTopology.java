package com.sdu.jstorm.transaction.trident.custom;

import com.sdu.jstorm.transaction.trident.custom.option.LocationUpdater;
import com.sdu.jstorm.transaction.trident.custom.spout.LocationSpout;
import com.sdu.jstorm.transaction.trident.custom.state.LocationDbFactory;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Locate Trident Topology
 *
 * @author hanhan.zhang
 * */
public class LocationTridentTopology {

    private static Fields fields = new Fields("userId", "location");

    private static List<Object>[] outputs = new List[] {new Values(1L, "北京"), new Values(2L, "上海"),
                                                        new Values(1L, "广东"), new Values(2L, "天津"),
                                                        new Values(3L, "澳门")};

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        Stream locationStream = topology.newStream("locations", new LocationSpout(2, fields, outputs));
        TridentState tridentState = locationStream.partitionPersist(new LocationDbFactory(), fields, new LocationUpdater());

//        TridentState tridentState = locationStream.persistentAggregate(new LocationDbFactory(), fields, new ReducerAggregator() {
//            @Override
//            public Object init() {
//                return null;
//            }
//
//            @Override
//            public Object reduce(Object curr, TridentTuple tuple) {
//                return null;
//            }
//        }, new Fields("locations"));

        // start topology locally
        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tridentTopology", conf, topology.build());

    }

}
