package com.sdu.jstorm.monitor.hook;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.jstorm.utils.JCollectionUtil;
import com.sdu.jstorm.utils.JGsonUtils;
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
 *  每个组件都会实例化{@link ITaskHook}, 而非单例
 *
 * @author hanhan.zhang
 * */
public class JDefaultTaskHook implements ITaskHook, Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDefaultTaskHook.class);

    private Map<Integer, String> componentToTask;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        if (componentToTask == null) {
            componentToTask = Maps.newConcurrentMap();
        }
        this.componentToTask = context.getTaskToComponent();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void emit(EmitInfo info) {
        // 发送组件信息
        int taskId = info.taskId;
        String emitComponentName = this.componentToTask.get(taskId);
        String emitStreamName = info.stream;
        List<Object> emitTuple = info.values;

        // 接收组件信息
        Map<String, Collection<Integer>> consumeComponentToTask = Maps.newHashMap();
        info.outTasks.forEach(consumeTask -> {
            String consumeComponent = this.componentToTask.get(consumeTask);
            Collection<Integer> consumeTaskSet = consumeComponentToTask.get(consumeComponent);
            if (consumeTaskSet == null) {
                consumeTaskSet = Sets.newHashSet(consumeTask);
                consumeComponentToTask.put(consumeComponent, consumeTaskSet);
            } else {
                consumeTaskSet.add(consumeTask);
            }
        });

        if (JCollectionUtil.isNotEmpty(consumeComponentToTask)) {
            consumeComponentToTask.forEach((component, tasks) -> {
                if (JCollectionUtil.isNotEmpty(tasks)) {
                    tasks.forEach(consumeTaskId -> {
                        LOGGER.info("Storm Component[{}/{}] Send Tuple : toComponent = {}/{}, stream = {}, tuple = {}",
                                    emitComponentName, taskId, component, consumeTaskId, emitStreamName, emitTuple);
                    });
                }
            });
        }
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
        LOGGER.info("spout ack : {}", JGsonUtils.formatJson(info));
    }

    @Override
    public void spoutFail(SpoutFailInfo info) {
        LOGGER.info("spout failure : {}", JGsonUtils.formatJson(info));
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
        LOGGER.info("bolt execute : {}", JGsonUtils.formatJson(info));
    }

    @Override
    public void boltAck(BoltAckInfo info) {
        LOGGER.info("bolt ack : {}", JGsonUtils.formatJson(info));
    }

    @Override
    public void boltFail(BoltFailInfo info) {
        LOGGER.info("bolt failure : {}", JGsonUtils.formatJson(info));
    }
}
