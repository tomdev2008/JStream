package com.sdu.jstorm.data.internal;

/**
 * @author hanhan.zhang
 * */
public interface JDataSource<T> {

    void start();

    T nextData();

    void commit(String key);

    void close();

}
