package com.sdu.jstorm.translator;

import com.sdu.jstorm.tuple.JStreamTuple;
import org.apache.storm.tuple.Tuple;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public interface JStreamTranslator extends JTranslator{

    List<JStreamTuple> apply(Tuple tuple);
}
