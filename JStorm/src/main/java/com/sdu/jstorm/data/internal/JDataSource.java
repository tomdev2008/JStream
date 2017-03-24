package com.sdu.jstorm.data.internal;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface JDataSource<T> extends Serializable {

    void start();

    T nextData();

    void commit(String key);

    void close();

}
