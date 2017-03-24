package com.sdu.jstorm.translator;

import com.sdu.jstorm.tuple.JStreamTuple;

/**
 * @author hanhan.zhang
 * */
public interface JDataTranslator<T> extends JTranslator {

    String getCommitKey(T data);

    JStreamTuple apply(T data);

    boolean isDirectFor(String stream);



}
