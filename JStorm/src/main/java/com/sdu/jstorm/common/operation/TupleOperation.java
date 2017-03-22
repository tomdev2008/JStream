package com.sdu.jstorm.common.operation;

import org.apache.storm.task.OutputCollector;

import java.io.Serializable;

/**
 * Tuple Process
 *
 * @author hanhan.zhang
 * */
public interface TupleOperation<T> extends Serializable {

    public void process(OutputCollector collector, T tuple);

}
