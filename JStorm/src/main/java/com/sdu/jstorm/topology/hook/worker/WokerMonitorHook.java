package com.sdu.jstorm.topology.hook.worker;

import com.sdu.jstorm.utils.PrintUtils;
import org.apache.storm.hooks.IWorkerHook;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 工作节点监控({@link IWorkerHook}继承Java序列化接口{@link java.io.Serializable)
 *
 * Note :
 *      属性必须可序列化(在{@link TopologyBuilder#createTopology()}中需要序列化)
 *
 * @author hanhan.zhang
 * */
public class WokerMonitorHook implements IWorkerHook {

    private static final Logger LOGGER = LoggerFactory.getLogger(WokerMonitorHook.class);

    private int _workPort;

    private List<Integer> _taskList;

    private Map<String, List<Integer>> _componentToTask;

    @Override
    public void start(Map stormConf, WorkerTopologyContext context) {
        // 工作节点端口
        this._workPort = context.getThisWorkerPort();
        this._taskList = context.getThisWorkerTasks();

        // 工作节点上部署的组件
        this._componentToTask = new HashMap<>();
        context.getTaskToComponent().forEach(((task, component) -> {
            if (this._taskList.contains(task)) {
                List<Integer> componentTask = this._componentToTask.get(component);
                if (componentTask == null) {
                    componentTask = new ArrayList<Integer>();
                    componentTask.add(task);
                    this._componentToTask.put(component, componentTask);
                } else {
                    componentTask.add(task);
                }
            }
        }));

        this._componentToTask.forEach((component, task) -> LOGGER.info("工作节点端口:{}, 部署组件:{} --> {}", this._workPort, component, PrintUtils.collectionToString(task, "[", "]", "\t")));

    }



    @Override
    public void shutdown() {

    }
}
