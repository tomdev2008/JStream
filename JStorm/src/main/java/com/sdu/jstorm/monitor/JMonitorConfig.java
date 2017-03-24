package com.sdu.jstorm.monitor;

import lombok.Getter;
import lombok.Setter;
import org.apache.storm.thrift.protocol.TProtocolFactory;

/**
 * @author hanhan.zhang
 * */
public class JMonitorConfig {

    @Setter
    @Getter
    private String nimbusHost;
    @Setter
    @Getter
    private int nimbusPort;

    @Setter
    @Getter
    private int clientTimeout;

    @Setter
    @Getter
    private boolean isAsyncMonitor;

    @Setter
    @Getter
    private TProtocolFactory protocolFactory;


}
