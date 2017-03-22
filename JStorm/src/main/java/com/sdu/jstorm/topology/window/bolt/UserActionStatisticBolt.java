package com.sdu.jstorm.topology.window.bolt;

import com.sdu.jstorm.common.operation.TupleOperation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

/**
 * see {@link BaseWindowedBolt}
 *
 *
    |---------------------------------------------------------------------|
    | WindowLength = 7, SlideLength = 1                                   |                             |
    |      tuple1, tuple2, tuple3, tuple4, tuple5, tuple6, tuple7, tuple8 |
    |        |        |                                       |       |   |
    |        |________|____________Window1____________________|       |   |
    |                 |_____________________Window2___________________|   |
    |---------------------------------------------------------------------|
 *
 * @author hanhan.zhang
 * */
public class UserActionStatisticBolt extends BaseWindowedBolt {

    private OutputCollector _collector;

    private TupleOperation _operation;

    public UserActionStatisticBolt(TupleOperation windowTupleOperation) {
        this._operation = windowTupleOperation;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this._collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        this._operation.process(this._collector, inputWindow);
    }

}
