package com.sdu.jstorm.tuple;

import lombok.AllArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
@Setter
@AllArgsConstructor
public class JStreamTuple {

    private List<Object> tuple;

    private String stream;

    public List<Object> streamTuple() {
        return tuple;
    }

    public String tupleStream() {
        return stream;
    }

}
