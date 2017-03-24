package com.sdu.jstorm.translator;

import org.apache.storm.tuple.Fields;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public interface JTranslator extends Serializable {

    List<String> DEFAULT_STREAM = Collections.singletonList("default");

    Fields getFieldsFor(String stream);

    default List<String> streams() {
        return DEFAULT_STREAM;
    }
}
