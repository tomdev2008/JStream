package com.sdu.jstorm.data;

import com.sdu.jstorm.tuple.JStreamTuple;
import lombok.Getter;

/**
 * @author hanhan.zhang
 * */
@Getter
public class JDataMessageId {

    private String  commitKey;

    private JStreamTuple tuple;

    private int failure;

    public JDataMessageId(String commitKey, JStreamTuple tuple) {
        this.commitKey = commitKey;
        this.tuple = tuple;
        this.failure = 0;
    }

    public void incrementFailure() {
        ++failure;
    }

    public int getFailure() {
        return failure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JDataMessageId that = (JDataMessageId) o;

        return tuple != null ? tuple.equals(that.tuple) : that.tuple == null;

    }

    @Override
    public int hashCode() {
        return tuple != null ? tuple.hashCode() : 0;
    }
}
