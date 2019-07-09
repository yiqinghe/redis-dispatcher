package com.demo;

import java.util.List;
import java.util.Queue;

/**
 * Created by caigaonian on 17/11/16.
 */
public interface ITask <E> {

	public final String INIT_STATUS = "init";
	public final String RUNINNG_STATUS = "running";
	public final String DONE_STATUS = "done";

	/**
	 * 获取批量任务列表，注意hold住异常
	 * @return
	 */
	public List<ITask> buildBatchTask();

	/**
	 * 批量任务的batch id，例如planId，直接return batchId，batchId 为获取批量任务getBatchTask的时候设置
	 * @return
	 */
	public String buildBatchId(List<ITask> taskList);

	/**
	 * 根据入参，返回task实例
	 * @return
	 */
	public ITask<E> buildTask(String extData);

	/**
	 * 设置附加信息，例如规则、上下文信息等，，extData由每次需要发送心跳、更新上下文信息到redis的时候被设置、调用
	 * 并且返回附加信息
	 * @return
	 */
	public String buildExtData(ITask<E> task);

	/**
	 * 获取数据队列，由task worker 实现获取具体数据队列的逻辑
	 * @return
	 */
	public Queue<E> buildDataQueue(ITask<E> task);

	/**
	 * 当没有任务时，构造返回一个空队列
	 * @return
	 */
	public Queue<E> buildEmptyQueue();

	/**
	 * 更新任务批次状态，每次一批任务完成后，被TaskMaster调用
	 * @param batchId
	 * @return
	 */
	public boolean updateBatchTaskStatus(String batchId);

	/**
	 * 判断任务是否完成，由 task worker 实现判断逻辑
	 * @return
	 */
	public boolean isTaskDone(ITask<E> task);

	/**
	 * 获取rediskey的前缀，避免多类任务冲突
	 * @return
	 */
	public String getRedisKeyPrefix();


	/**
	 * 获取保活心跳的发送间隔，默认是10秒
	 * @return
	 */
	public int getHeatbeatIntervalSeconds();

	/**
	 * 获取最大的保活心跳，lost 心跳的间隔，如果超过任务会被释放，默认是300秒
	 * @return
	 */
	public int getHeatbeatLostSeconds();

	public String getTaskId();

	public String getBatchId();

	public String getExtData();

	public String getStatus();

	public void setTaskId(String taskId);

	public void setBatchId(String batchId);

	public void setExtData(String extData);

	public void setStatus(String status);

}
