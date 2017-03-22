package com.sdu.jstorm.topology.hook.task;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.info.*;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Storm Task监控
 *
 * Note :
 *  每个组件都会实例化{@link ITaskHook},而非单例
 *
 * @author hanhan.zhang
 * */
public class TaskMonitorHook implements ITaskHook, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskMonitorHook.class);

    private String _componentName;

    private Integer _taskId;

    private Map<Integer, String> _componentToTaskMap;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        if (_componentToTaskMap == null) {
            _componentToTaskMap = Maps.newConcurrentMap();
        }
        this._componentName = context.getThisComponentId();
        this._taskId = context.getThisTaskId();
        this._componentToTaskMap = context.getTaskToComponent();
        LOGGER.info("初始化组件{}编号{}的Task对象", this._componentName, this._taskId);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void emit(EmitInfo info) {
        String emitComponentName = this._componentToTaskMap.get(info.taskId);
        String emitStreamName = info.stream;
        Map<String, Collection<Integer>> consumeComponentToTask = Maps.newHashMap();
        info.outTasks.forEach(consumeTask -> {
            String consumeComponent = this._componentToTaskMap.get(consumeTask);
            Collection<Integer> consumeTaskSet = consumeComponentToTask.get(consumeComponent);
            if (consumeTaskSet == null) {
                consumeTaskSet = Sets.newHashSet(consumeTask);
                consumeComponentToTask.put(consumeComponent, consumeTaskSet);
            } else {
                consumeTaskSet.add(consumeTask);
            }
        });
        List<Object> emitTuple = info.values;
        LOGGER.info("组件{}发送{}消息流给组件{}消息: {} ", emitComponentName, emitStreamName, consumeComponentToTask, emitTuple);
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {

    }

    @Override
    public void spoutFail(SpoutFailInfo info) {

    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {

    }

    @Override
    public void boltAck(BoltAckInfo info) {

    }

    @Override
    public void boltFail(BoltFailInfo info) {

    }
}
