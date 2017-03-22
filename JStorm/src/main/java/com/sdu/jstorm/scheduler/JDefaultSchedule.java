package com.sdu.jstorm.scheduler;

import com.sdu.jstorm.utils.CollectionUtil;
import com.sdu.jstorm.utils.Const;
import org.apache.storm.scheduler.*;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Storm Task Scheduler(storm.scheduler=JDefaultSchedule)
 *
 * @author hanhan.zhang
 * */
public class JDefaultSchedule implements IScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(JDefaultSchedule.class);

    private boolean _useDefault;

    @Override
    public void prepare(Map conf) {
        this._useDefault = (boolean) conf.getOrDefault(Const.STORM_USE_DEFAULT_SCHEDULE, false);
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        if (this._useDefault) {
            LOGGER.info("采用默认拓扑调度器");
            new EvenScheduler().schedule(topologies, cluster);
        } else {
            LOGGER.info("采用自定义拓扑调度器");
            Collection<TopologyDetails> topologyDetails = topologies.getTopologies();
            if (CollectionUtil.isNotEmpty(topologyDetails)) {
                topologyDetails.forEach(topologyDetail -> this.scheduleTopology(cluster, topologyDetail));
            }
        }
    }

    // 拓扑调度实现
    private void scheduleTopology(Cluster cluster, TopologyDetails topologyDetail) {
        String topologyName = topologyDetail.getName();
        LOGGER.info("拓扑[{}]申请分配资源", topologyName);
        boolean needsScheduling = cluster.needsScheduling(topologyDetail);
        if (needsScheduling) {
            Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topologyDetail);
            this.scheduleComponent(cluster, topologyDetail, componentToExecutors);
        } else {
            SchedulerAssignment schedulerAssignment = cluster.getAssignmentById(topologyDetail.getId());
            Map<ExecutorDetails, WorkerSlot> executorToWorkerSlots = schedulerAssignment.getExecutorToSlot();
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToWorkerSlots.entrySet()) {
                ExecutorDetails executorDetail = entry.getKey();
                WorkerSlot workSlot = entry.getValue();
                if (first) {
                    sb.append("Executor:").append(executorDetail.toString()).append("-->").append("Worker:[").append(workSlot.toString()).append("]");
                    first = false;
                } else {
                    sb.append(",").append("Executor:").append(executorDetail.toString()).append("-->").append("Worker:[").append("[").append(workSlot.toString()).append("]");
                }
            }
            LOGGER.info("拓扑[{}]资源调度信息: {}", topologyName, sb.toString());
        }
    }

    // 系统资源分配
    private void scheduleComponent(Cluster cluster, TopologyDetails topologyDetail, Map<String, List<ExecutorDetails>> componentToExecutorDetails) {
        // 系统可分配Worker并对可用Worker排序(先按照port升序,再按照nodeId升序)
        List<WorkerSlot> assignableWorkerSlot = cluster.getAvailableSlots();
        Collections.sort(assignableWorkerSlot, (workerSlot1, workerSlot2) -> {
            if (workerSlot1.getPort() == workerSlot2.getPort()) {
                return workerSlot1.getNodeId().compareTo(workerSlot2.getNodeId());
            }
            return workerSlot1.getPort() - workerSlot2.getPort();
        });

        // 待分配的Executor,并按照ExecutorDetails的startTask升序排序
        List<ExecutorDetails> needScheduleExecutors = Lists.newArrayList();
        componentToExecutorDetails.forEach((componentId, executorDetail) -> needScheduleExecutors.addAll(executorDetail));
        Collections.sort(needScheduleExecutors, (executorDetail1, executorDetail2) -> executorDetail1.getStartTask() - executorDetail2.getStartTask());

        // 待分配Worker数目
        int desiredNumWorkers = topologyDetail.getNumWorkers();

        // 划分每个Worker分配的Executor数目
        ArrayList<Integer> allocateDetail = Lists.newArrayListWithCapacity(desiredNumWorkers);
        int average = needScheduleExecutors.size() / desiredNumWorkers;
        int remainder = needScheduleExecutors.size() % desiredNumWorkers;
        for (int i = 0; i < desiredNumWorkers; i++) {
            if (remainder == 0) {
                allocateDetail.add(average);
            } else {
                allocateDetail.add(average + 1);
                --remainder;
            }
        }

        // Executor分配到Worker
        int index = 0;
        for (int i = 0; i < desiredNumWorkers; i++) {
            WorkerSlot workerSlot = assignableWorkerSlot.get(i);
            int executorNumber = allocateDetail.get(i);
            Collection<ExecutorDetails> executors = needScheduleExecutors.subList(index, executorNumber);
            index += executorNumber;
            cluster.assign(workerSlot, topologyDetail.getId(), executors);
            LOGGER.info("拓扑[{}]资源调度信息: Executor{} --> Worker[{}]", topologyDetail.getName(), executors, workerSlot);
        }

    }
}
