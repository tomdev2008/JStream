package com.sdu.jstorm.kafka;

import lombok.AllArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
@Setter
@AllArgsConstructor
public class JKafkaTuple {

    private List<Object> tuple;

    private String stream;

    public List<Object> kafkaTuple() {
        return tuple;
    }

    public String tupleStream() {
        return stream;
    }

}
