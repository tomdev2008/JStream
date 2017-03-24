package com.sdu.jstorm.tuple;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class JStreamTuple {

    private List<Object> tuple;

    private String stream;

    public List<Object> tuple() {
        return tuple;
    }

    public String stream() {
        return stream;
    }

}
