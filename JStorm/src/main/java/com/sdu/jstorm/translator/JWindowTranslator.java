package com.sdu.jstorm.translator;

import com.sdu.jstorm.tuple.JStreamTuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public interface JWindowTranslator extends JTranslator {

    List<JStreamTuple> apply(TupleWindow tupleWindow);

}
