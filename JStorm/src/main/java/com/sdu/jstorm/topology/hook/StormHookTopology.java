package com.sdu.jstorm.topology.hook;

import com.google.common.collect.Lists;
import com.sdu.jstorm.common.FixedCycleSpout;
import com.sdu.jstorm.common.operation.impl.CycleTupleGenerator;
import com.sdu.jstorm.topology.group.router.bolt.MultiStreamBolt;
import com.sdu.jstorm.topology.group.router.bolt.StreamPrintBolt;
import com.sdu.jstorm.topology.group.router.help.StreamDesc;
import com.sdu.jstorm.topology.hook.task.TaskMonitorHook;
import com.sdu.jstorm.topology.hook.worker.WokerMonitorHook;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * Storm工作节点和任务监控拓扑
 *
 * @author hanhan.zhang
 * */
public class StormHookTopology {

    public static void main(String[] args) {
        // builder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 设置工作节点监控
        topologyBuilder.addWorkerHook(new WokerMonitorHook());

        String requestStreamId = "topology.request.stream";
        String requestFlag = "request";
        String responseStreamId = "topology.response.stream";
        String responseFlag = "response";
        List<StreamDesc> streamDescs = Lists.newArrayList(StreamDesc.builder()
                                                                    .streamId(requestStreamId)
                                                                    .flag(requestFlag)
                                                                    .direct(false)
                                                                    .fields(new Fields("log"))
                                                                    .build(),
                                                          StreamDesc.builder()
                                                                    .streamId(responseStreamId)
                                                                    .flag(responseFlag)
                                                                    .direct(false)
                                                                    .fields(new Fields("log"))
                                                                  .build());

        // spout
        ArrayList<List<Object>> dataSource = Lists.newArrayList(new Values("request: request log from ip 192.12.1.67"),
                                                            new Values("request: request log from ip 192.124.13.69"),
                                                            new Values("response: response log from ip 80.127.13.69"),
                                                            new Values("response: response log from ip 10.126.13.69"));
        String spoutStreamId = "topology.spout.stream";
        FixedCycleSpout cycleSpout = new FixedCycleSpout(spoutStreamId, false, new Fields("log"), new CycleTupleGenerator(dataSource));
        topologyBuilder.setSpout ("log.cycle.spout", cycleSpout, 1);


        // bolt
        MultiStreamBolt multiStreamBolt = new MultiStreamBolt(streamDescs);
        StreamPrintBolt requestPrintBolt = new StreamPrintBolt(requestStreamId, false);
        StreamPrintBolt responsePrintBolt = new StreamPrintBolt(responseStreamId, false);
        topologyBuilder.setBolt("multiple.stream.bolt", multiStreamBolt, 1)
                        .shuffleGrouping("log.cycle.spout", spoutStreamId);

        topologyBuilder.setBolt("request.print.bolt", requestPrintBolt, 1)
                        .shuffleGrouping("multiple.stream.bolt", requestStreamId);
        topologyBuilder.setBolt("response.print.bolt", responsePrintBolt, 1)
                        .shuffleGrouping("multiple.stream.bolt", responseStreamId);


        Config config = new Config();
        // 设置Task监控
        config.put(Config.TOPOLOGY_AUTO_TASK_HOOKS, Lists.newArrayList(TaskMonitorHook.class.getName()));
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("multiSteamRouteTopology", config, topologyBuilder.createTopology());

    }

}
