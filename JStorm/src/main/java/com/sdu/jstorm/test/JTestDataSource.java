package com.sdu.jstorm.test;

import com.sdu.jstorm.data.internal.JDataSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hanhan.zhang
 * */
public class JTestDataSource implements JDataSource<JInputData<String>> {

    private List<JInputData<String>> dataSource;


    private AtomicInteger index;

    public JTestDataSource(List<JInputData<String>> dataSource) {
        this.dataSource = dataSource;
        this.index = new AtomicInteger(0);
    }

    @Override
    public void start() {

    }

    @Override
    public JInputData<String> nextData() {
        sleep();
        int pos = index.getAndIncrement() % dataSource.size();
        return dataSource.get(pos);
    }

    @Override
    public void commit(String key) {

    }

    @Override
    public void close() {

    }

    private void sleep() {
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
