package com.sdu.jstorm.monitor;

import com.sdu.jstorm.utils.JCollectionUtil;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.async.AsyncMethodCallback;
import org.apache.storm.thrift.async.TAsyncClientManager;
import org.apache.storm.thrift.protocol.TBinaryProtocol;
import org.apache.storm.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;

/**
 * 通过Thrift API与Storm集群管理节点Nimbus交互:
 *
 * 1: Storm集群信息{@link Nimbus.Client#getClusterInfo()}
 *
 * 2: Storm拓扑并发度调整{@link Nimbus.Client#rebalance(String, RebalanceOptions)}
 *
 * 3: Storm拓扑关闭{@link Nimbus.Client#killTopologyWithOpts(String, KillOptions)}
 *
 * 4: Storm拓扑组件信息{@link Nimbus.Client#getComponentPageInfo(String, String, String, boolean)}
 *
 * @author hanhan.zhang
 * */
public class JTopologyMonitor implements JStormMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JTopologyMonitor.class);

    private JMonitorConfig monitorConfig;

    private Nimbus.Client nimbusClient;

    private Nimbus.AsyncClient nimbusAsyncClient;

    public JTopologyMonitor(JMonitorConfig monitorConfig) {
        this.monitorConfig = monitorConfig;
    }

    @Override
    public void start() throws Exception {
        if (monitorConfig.isAsyncMonitor()) {
            TAsyncClientManager clientManager = new TAsyncClientManager();
            TNonblockingTransport transport = new TNonblockingSocket(monitorConfig.getNimbusHost(), monitorConfig.getNimbusPort(), monitorConfig.getClientTimeout());
            nimbusAsyncClient = new Nimbus.AsyncClient(monitorConfig.getProtocolFactory(), clientManager, transport);
        } else {
            TSocket socket = new TSocket(monitorConfig.getNimbusHost(), monitorConfig.getNimbusPort());
            TTransport transport = new TFramedTransport(socket, monitorConfig.getClientTimeout());
            nimbusClient = new Nimbus.Client(monitorConfig.getProtocolFactory().getProtocol(transport));
            socket.open();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void monitor() throws Exception {
        if (monitorConfig.isAsyncMonitor()) {
            doAsyncMonitor();
        } else {
            doSyncMonitor();
        }
    }

    private void doAsyncMonitor() throws TException {
        nimbusAsyncClient.getClusterInfo(new AsyncMethodCallbackImpl());
    }

    private void doSyncMonitor() throws TException {
        ClusterSummary clusterSummary = nimbusClient.getClusterInfo();
        doMonitor(clusterSummary);
    }

    private void doMonitor(ClusterSummary clusterSummary) {
        List<SupervisorSummary> supervisors = clusterSummary.get_supervisors();
        if (JCollectionUtil.isNotEmpty(supervisors)) {
            supervisors.forEach(supervisor -> {
                String host = supervisor.get_host();
                String supervisorId = supervisor.get_supervisor_id();
                int workers = supervisor.get_num_workers();
                int usedWorkers = supervisor.get_num_used_workers();
                double usedCpu = supervisor.get_used_cpu();
                double usedMemory = supervisor.get_used_mem();
                LOGGER.info("Worker[{}/{}]节点信息: slots = {}, usedSlots = {}, usedCpu = {}, usedMemory = {}",
                        host, supervisorId, workers, usedWorkers, usedCpu, usedMemory);
            });

        }

        List<TopologySummary> topologySummaries = clusterSummary.get_topologies();
        if (JCollectionUtil.isNotEmpty(topologySummaries)) {
            topologySummaries.forEach(topology -> {
                String topologyId = topology.get_id();
                String topologyName = topology.get_name();
                int taskNumber = topology.get_num_tasks();
                int executors = topology.get_num_executors();
                int workers = topology.get_num_workers();
                double requestMemoryOffHeap = topology.get_requested_memoffheap();
                double assignMemoryOffHeap = topology.get_assigned_memoffheap();
                double requestMemoryHeap = topology.get_requested_memonheap();
                double assignMemoryHeap = topology.get_assigned_memonheap();
                double assignCpu = topology.get_assigned_cpu();
                double requestCpu = topology.get_requested_cpu();

                LOGGER.info("Topology[{}/{}]信息: workerNum = {},  executorNum = {}, taskNum = {}, requestMemoryOffHeap = {}, " +
                            "assignMemoryOffHeap = {}, requestMemoryHeap = {}, assignMemoryHeap = {}, requestCpu = {}, assignCpu = {}",
                            topologyName, topologyId, workers, executors, taskNumber, requestMemoryOffHeap,
                            assignMemoryOffHeap, requestMemoryHeap, assignMemoryHeap, requestCpu, assignCpu);
            });
        }
    }

    private class AsyncMethodCallbackImpl implements AsyncMethodCallback<ClusterSummary> {
        @Override
        public void onComplete(ClusterSummary clusterSummary) {
            doMonitor(clusterSummary);
        }

        @Override
        public void onError(Exception e) {
            LOGGER.error("异步监控Storm集群信息异常", e);
        }
    }

    public static void main(String[] args) throws Exception {
        String ip = InetAddress.getLocalHost().getHostAddress();
        System.out.println(ip);

        // Storm默认加载default.yaml配置
        JMonitorConfig monitorConfig = new JMonitorConfig();
        monitorConfig.setNimbusHost(ip);
        monitorConfig.setNimbusPort(54816);
        monitorConfig.setClientTimeout(1000);
        monitorConfig.setProtocolFactory(new TBinaryProtocol.Factory());
        monitorConfig.setAsyncMonitor(false);
        JTopologyMonitor topologyMonitor = new JTopologyMonitor(monitorConfig);

        topologyMonitor.start();
        topologyMonitor.monitor();
    }
}
