package com.sdu.jstorm.monitor.hook;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.jstorm.utils.PrintUtils;
import org.apache.storm.hooks.IWorkerHook;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 工作节点监控({@link IWorkerHook}继承Java序列化接口{@link java.io.Serializable)
 *
 * Note :
 *      属性必须可序列化(在{@link TopologyBuilder#createTopology()}中需要序列化)
 *
 * @author hanhan.zhang
 * */
public class JDefaultWorkerHook implements IWorkerHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDefaultWorkerHook.class);

    @Override
    public void start(Map stormConf, WorkerTopologyContext context) {
        // 工作节点端口
        int workPort = context.getThisWorkerPort();

        List<Integer> targetTasks = context.getThisWorkerTasks();
        Map<Integer, String> taskToComponent = context.getTaskToComponent();

        // 该Worker工作节点部署的组件
        Map<String, Collection<Integer>> deployComponentToTask = Maps.newHashMap();
        targetTasks.forEach(task -> {
            String component = taskToComponent.get(task);
            Collection<Integer> deployTask = deployComponentToTask.get(component);
            if (deployTask == null) {
                deployTask = Sets.newHashSet();
                deployTask.add(task);
                deployComponentToTask.put(component, deployTask);
            } else {
                deployTask.add(task);
            }
        });


        LOGGER.info("Storm worker deploy component : port = {}, deployComponentToTask = {}", workPort, deployComponentToTask);
    }



    @Override
    public void shutdown() {

    }
}
