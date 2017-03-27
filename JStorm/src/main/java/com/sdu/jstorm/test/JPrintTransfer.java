package com.sdu.jstorm.test;

import org.apache.storm.tuple.Tuple;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface JPrintTransfer extends Serializable {

    Object transfer(Tuple inputTuple);

}
