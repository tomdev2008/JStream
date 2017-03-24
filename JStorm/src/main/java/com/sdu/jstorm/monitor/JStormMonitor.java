package com.sdu.jstorm.monitor;

/**
 * @author hanhan.zhang
 * */
public interface JStormMonitor {

    void start() throws Exception;

    void close();

    void monitor() throws Exception ;

}
