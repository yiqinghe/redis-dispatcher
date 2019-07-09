package com.demo;

import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;
import java.util.Queue;
import java.util.Random;

/**
 * Created by caigaonian on 17/11/16.
 */
public abstract class TaskMaster<E> implements ITask {

    private static Logger logger = LoggerFactory.getLogger("customFileLogger");


    private final String PENDING_TASK_LIST_LOCK_KEY = getRedisKeyPrefix() + "pending.task.list.lock.key";
    private final String PENDING_TASK_LIST_KEY = getRedisKeyPrefix() + "pending.task.list.key";

    private final int redisExpireTimeSecond = 120 * 60 * 60;

    protected JedisCommands jedisCommands;

    // 由taskMaster负责初始化，传入
    protected String taskId = null;

    // 额外信息，例如规则、上下文等，由每次需要发送心跳、更新上下文信息到redis的时候设置
    protected String extData = null;

    // 批量id，获取批量任务的时候设置
    protected String batchId = null;

    // 任务状态
    protected String status;

    // 由master先初始化任务队列到redis，然后在从redis任务队列中取出初始化的。
    private volatile ITask task;

    private final String SPLIT = "@_";

    /**
     * 上一次发送心跳时间
     */
    private long lastHeatbeatTimeMs;

    public TaskMaster(JedisCommands jedisCommands) {
        this.jedisCommands = jedisCommands;
    }

    /**
     * 核心逻辑：获取queue，在此方法包含发送保活心跳,
     * 调用此方法间隔必须在心跳间隔时间与最大心跳间隔时间之内，否则任务会被释放
     *
     * @return
     * @throws AllBatchTaskDoneException
     */
    public Queue<E> getQueue() throws AllBatchTaskDoneException {
        // 发送保活心跳
        heartbeat();

        if (task != null && !task.isTaskDone(task)) {
            Queue queue = task.buildDataQueue(task);
            return queue;
        } else {
            String timeStamp = String.valueOf(System.currentTimeMillis());
            if (tryLock(timeStamp)) {
                try {
                    // 再判断是否需要置位done状态
                    updateRedisTaskDone();
                    // 判断是否队列为空
                    long pendingLength = jedisCommands.llen(PENDING_TASK_LIST_KEY);
                    logger.info(Thread.currentThread().getName() + "------pendingLength-----" + pendingLength + ", key = " + PENDING_TASK_LIST_KEY);
                    if (pendingLength == 0) {
                        logger.info(Thread.currentThread().getName() + " pendingLen is null, time= " + System.currentTimeMillis());
                        // 获取下个批量任务
                        List<ITask> taskList = getTasks();
                        logger.debug(Thread.currentThread().getName() + "taskList------------ " + (taskList == null ? null : taskList.toString()));
                        if (taskList == null || taskList.isEmpty()) {
                            // 睡眠，直接返回空队列
                            logger.debug("tasklist is empty");
                            threadSleep();
                            throw new AllBatchTaskDoneException();
                        } else {
                            pendingLength = taskList.size();
                            // 初始化心跳时间、任务状态等。
                            for (int i = (int) pendingLength - 1; i >= 0; i--) {
                                ITask<E> tmpTask = taskList.get(i);
                                int taskId = i;
                                String status = INIT_STATUS;
                                String extData = tmpTask.getExtData();
                                logger.debug(Thread.currentThread().getName() + " String extData=tmpTask.getExtData();    " + extData);
                                String batchId = tmpTask.getBatchId();
                                long now = System.currentTimeMillis();
                                String taskInfoRedis = buildRedisTaskInfo(String.valueOf(taskId), status, extData,
                                        now, batchId);
                                // 逆序插入队列
                                jedisCommands.lpush(PENDING_TASK_LIST_KEY, taskInfoRedis);
                            }
                            jedisCommands.expire(PENDING_TASK_LIST_KEY, redisExpireTimeSecond);
                        }
                    }
                    // 获取所有任务，判断状态以及心跳时间
                    List<String> taskList = jedisCommands.lrange(PENDING_TASK_LIST_KEY, 0, pendingLength - 1);
                    int doneNum = 0;
                    String batchId = null;
                    for (String taskInfo : taskList) {
                        String[] taskInfos = taskInfo.split(SPLIT);
                        if (taskInfos.length >= 5) {
                            String taskId = taskInfos[0];
                            String status = taskInfos[1];
                            String extData = taskInfos[2];
                            long heatbeatTimeMs = Long.valueOf(taskInfos[3]);
                            batchId = taskInfos[4];

                            if (DONE_STATUS.equals(status)) {
                                doneNum++;
                            }
                            long now = System.currentTimeMillis();
                            long elapseTime = heatbeatTimeMs + getHeatbeatLostSeconds() * 1000;
                            if (INIT_STATUS.equals(status) || (RUNINNG_STATUS.equals(status) &&
                                    elapseTime < now)) {
                                // 刚初始化的或者lost heartbeat。
                                logger.info(Thread.currentThread().getName() + " task = buildTask(extData);    " + extData
                                        + ", elapseTime=" + elapseTime + ", now=" + now + ", status=" + status + " task=" + taskInfo);
                                if (extData == null || extData.equals("null")) {
                                    // 直接置成完成,无效，告警出来
                                    logger.error(Thread.currentThread().getName() + " extDataIsNullException " + ",task=" + taskInfo);
                                    jedisCommands.lset(PENDING_TASK_LIST_KEY, Integer.valueOf(taskId),
                                            buildRedisTaskInfo(taskId, DONE_STATUS, extData, now, batchId));
                                } else {
                                    task = buildTask(extData);
                                    task.setTaskId(taskId);
                                    task.setBatchId(batchId);
                                    task.setStatus(RUNINNG_STATUS);
                                    // bugfix 立马更新心跳时间
                                    jedisCommands.lset(PENDING_TASK_LIST_KEY, Integer.valueOf(task.getTaskId()),
                                            buildRedisTaskInfo(task.getTaskId(), RUNINNG_STATUS, task.getExtData(), now, task.getBatchId()));
                                }
                                break;
                            }
                        } else {
                            logger.warn(Thread.currentThread().getName() + "taskInfos.length < 5 batchID  taskList:{}", taskList);
                            //System.out.println("taskInfos.length < 5 batchID  taskList:{}"+taskList);
                            return buildEmptyQueue();
                        }
                    }
                    // 所有任务都是done
                    if (doneNum == pendingLength) {
                        logger.info(Thread.currentThread().getName() + ", doneNum == pendingLength batchID {}", batchId);
                        // 更新任务对应的批次的状态
                        updateBatchTaskStatus(batchId);
                        // 删除pending list
                        jedisCommands.del(PENDING_TASK_LIST_KEY);
                        // 重新初始化
                        task = null;
                        // 返回空队列，下次调用再填充任务
                        return buildEmptyQueue();

                    }

                    logger.debug(Thread.currentThread().getName() + " getQueue, ready to release lock");

                    // 如果所有任务都处于被运行的状态，会拿不到任务
                    return buildEmptyQueue();
                } finally {
                    // 释放锁
                    unlock(timeStamp);
                }
            } else {
                //睡眠，返回空队列
                //logger.info("thread:"+Thread.currentThread().getName()+",not get the lock");
                //System.out.println("thread:"+Thread.currentThread().getName()+",not get the lock");

                threadSleep();
                // fixme
                return buildEmptyQueue();
            }
        }
    }

    /**
     * 如果完成了，将任务置成完成
     */
    private void updateRedisTaskDone() {
        if (task != null && task.isTaskDone(task)) {
            logger.debug("thread:" + Thread.currentThread().getName() + ",task is done batchId {},taskId {}, task={}", task.getBatchId(), task.getTaskId(), task);
            //System.out.println("thread:"+Thread.currentThread().getName()+",task is done batchId {}"+task.getBatchId()+"taskId {}"+task.getTaskId());
            // 设置上下文等信息
            buildExtDataAndValidate(task);
            try {
                jedisCommands.lset(PENDING_TASK_LIST_KEY, Integer.valueOf(task.getTaskId()),
                        buildRedisTaskInfo(task.getTaskId(), DONE_STATUS, task.getExtData(), System.currentTimeMillis(), task.getBatchId()));
            } catch (JedisDataException e) {
                logger.info("thread:" + Thread.currentThread().getName() + " key={" + PENDING_TASK_LIST_KEY + "} not exists, maybe already been delete");
            }
            task = null;
        }
    }

    /**
     * 调用此方法发送保活心跳,以及附加信息
     */
    private void heartbeat() {
        // 判断间隔时间
        long now = System.currentTimeMillis();
        if (lastHeatbeatTimeMs + getHeatbeatIntervalSeconds() * 1000 < now) {
            lastHeatbeatTimeMs = now;
            logger.debug(Thread.currentThread().getName() + " in heartbeat... task=" + task);
            if (task != null) {
                // 设置上下文等信息
                updateExtData(task);
                try {
                    jedisCommands.lset(PENDING_TASK_LIST_KEY, Integer.valueOf(task.getTaskId()),
                            buildRedisTaskInfo(task.getTaskId(), RUNINNG_STATUS, task.getExtData(), now, task.getBatchId()));
                    logger.debug(Thread.currentThread().getName() + "task heartbeat batchId:{},taskId:{},task={}", task.getBatchId(), task.getTaskId(), task);

                } catch (Exception e) {
                    logger.warn(Thread.currentThread().getName() + " throw exception extData=" + task.getExtData()
                            + ", taskId=" + task.getTaskId() + ", batchId=" + task.getBatchId(), e);
                }
            }
        }
    }

    /**
     * 组装redis任务信息
     *
     * @param taskId
     * @param status
     * @param extData
     * @param heatbeatTimeMs
     * @param batchId
     * @return
     */
    public String buildRedisTaskInfo(String taskId, String status, String extData, long heatbeatTimeMs, String batchId) {
        return taskId + SPLIT + status + SPLIT + extData + SPLIT + heatbeatTimeMs + SPLIT + batchId;
    }

    /**
     * 获取分布式锁
     *
     * @return
     */
    public boolean tryLock(String timeStamp) {
        String result = jedisCommands.set(PENDING_TASK_LIST_LOCK_KEY, timeStamp,
                RedisConstants.SET_IF_NOT_EXIST,
                RedisConstants.SET_WITH_EXPIRE_SECONDS,
                300);
        if (RedisConstants.RESPONSE_SUCCESS.equals(result)) {
            return true;
        }
        return false;
    }

    public void unlock(String timeStamp) {
        String redisTimeStamp = jedisCommands.get(PENDING_TASK_LIST_LOCK_KEY);
        if (timeStamp.equals(redisTimeStamp)) {
            jedisCommands.del(PENDING_TASK_LIST_LOCK_KEY);
        }
    }

    /**
     * 休眠1ms+随机秒，直接返回空队列
     */
    private void threadSleep() {
        Random random = new Random();
        Utils.sleep(3 + random.nextInt(2));
    }

    public List<ITask> getTasks() {
        // setBatchId依赖getBatchTask();
        logger.info(Thread.currentThread().getName() + ", getTasks before");
        List<ITask> taskList = buildBatchTask();
        logger.info(Thread.currentThread().getName() + ", getTasks after tasks=" + taskList.toString());
        String batchId = buildBatchId(taskList);
        // 上下文信息一起初始化
        for (ITask task : taskList) {
            //fixme
            task.setBatchId(batchId);
            buildExtDataAndValidate(task);
        }
        return taskList;
    }

    /**
     * 不允许出现特殊分割符@@
     *
     * @param task
     */
    private void buildExtDataAndValidate(ITask task) throws IllegalArgumentException {
        logger.debug(Thread.currentThread().getName() + ", buildExtDataAndValidate task=" + task.toString());
        String extData = buildExtData(task);
        if (extData.contains(SPLIT)) {
            throw new IllegalArgumentException();
        }
        task.setExtData(extData);
        logger.debug(Thread.currentThread().getName() + ", buildExtDataAndValidate after task=" + task.toString() + " extData=" + extData);
    }

    private void updateExtData(ITask task) throws IllegalArgumentException {
        buildExtDataAndValidate(task);
    }

    @Override
    public int getHeatbeatIntervalSeconds() {
        return 10;
    }

    @Override
    public int getHeatbeatLostSeconds() {
        return 300;
    }

    @Override
    public String getBatchId() {
        return batchId;
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    @Override
    public String getExtData() {
        return extData;
    }

    @Override
    public String getStatus() {
        return status;
    }

    @Override
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    @Override
    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    @Override
    public void setExtData(String extData) {
        this.extData = extData;
    }

    @Override
    public void setStatus(String status) {
        this.status = status;
    }

    @Deprecated
    public ITask getTask() {
        return task;
    }
}
