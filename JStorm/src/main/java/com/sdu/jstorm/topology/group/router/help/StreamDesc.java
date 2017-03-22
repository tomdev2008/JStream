package com.sdu.jstorm.topology.group.router.help;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.storm.tuple.Fields;

import java.io.Serializable;

/**
 * Stream Description
 *
 * @author hanhan.zhang
 * */
@Setter
@Getter
@Builder
public class StreamDesc implements Serializable {

    // stream id
    private String streamId;

    // stream direct
    private boolean direct;

    // stream field
    private Fields fields;

    // stream flag
    private String flag;

    public boolean interest(String input) {
        return input.startsWith(flag);
    }
}
