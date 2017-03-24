package com.sdu.jstorm.translator;

import com.sdu.jstorm.tuple.JStreamTuple;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public interface JDataTranslator<T> extends JTranslator {

    String getCommitKey(T data);

    List<JStreamTuple> apply(T data);

    boolean isDirectFor(String stream);



}
