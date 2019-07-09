package com.demo;

import com.paic.smp.ubas.userTagTopology.entity.AllBatchTaskDoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by caigaonian on 17/11/21.
 */
public class TaskDemo<E> extends TaskMaster {
	private static Logger logger = LoggerFactory.getLogger("customFileLogger");

	private final String SPLIT="_";
	private int position ;
	private String rule;

	private Map<String,String> batchStatus = new HashMap<>();

	private static final int threadNum = 5;
	private static final int taskNumInOneBatch = 10;
	private static final int eachQueueDataSize = 1000;
	private static final int totalQueueDataSize = 10000;


	public static void main(String[] args) throws InterruptedException {
		// demo 模拟定时一直调用
		for(int j = 0;j <10000000;j++){
			// demo 模拟并发
			for(int i = 0; i< threadNum; i++){
				new Thread(){
					@Override
					public void run(){
						try {
							// 休眠随机秒
							Thread.sleep(1+Double.valueOf(2*Math.random()).intValue());
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						// fixme 为什么不能使用单例？
						final JedisCommands jedisCommands = new Jedis("localhost");
						TaskDemo<Data> task = new TaskDemo<Data>(jedisCommands);
						try {
							task.run();
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (AllBatchTaskDoneException ea) {
							ea.printStackTrace();
						}
					}
				}.start();
			}
			Thread.sleep(5000);
		}

	}

	public TaskDemo(JedisCommands jedisCommands) {
		super(jedisCommands);
	}

	/**
	 * 业务任务运行逻辑demo，如果一个queue运行完了继续填充queue，此方法可以被周期性的调用，看业务实现
	 * @throws InterruptedException
	 */
	public void run() throws InterruptedException,AllBatchTaskDoneException {
		System.out.println("time:"+System.currentTimeMillis()+"run >>>> thread:"+Thread.currentThread().getName());
		Queue dataQueue = getQueue();
		int count =0;
		while(!dataQueue.isEmpty()){
			count++;
			Data data = (Data)dataQueue.poll();
			Thread.sleep(1);
			// 抽样打日志
			if(count % (totalQueueDataSize /5) == 0){
				System.out.println("taskId:"+this.getTask().getTaskId()+",thread:"+Thread.currentThread().getName()+",run:"+data +",count:"+count);
			}
			if(dataQueue.isEmpty()){
				dataQueue = getQueue();
			}
		}
	}

	@Override
	public List<ITask> buildBatchTask() {
		// todo 获取新增任务 10个投放计划



		System.out.println("buildBatchTask >>>>");
		String batchId = "batchId_"+Double.valueOf(Math.random() *100000).toString();
		batchStatus.put(batchId,RUNINNG_STATUS);

		String batchCondition = Double.valueOf(Math.random() *100000).toString();
		List<ITask> taskList = new ArrayList<>();
		for(int i = 0 ;i < taskNumInOneBatch; i++){
			TaskDemo task = new TaskDemo(jedisCommands);
			//fixme
			task.setBatchId(batchId);
			task.setStatus(RUNINNG_STATUS);
			task.rule =  "sql:select * from xxx where batch="+batchCondition+" and task="+String.valueOf(i);
			task.position = 0;
			task.setExtData(buildExtData(task));
			taskList.add(task);
		}

		return taskList;
	}

	@Override
	public String buildBatchId(List list) {
		if(list!=null && !list.isEmpty()){
			TaskDemo taskDemo = (TaskDemo)list.get(0);
			return taskDemo.batchId;
		}else{
			//fixme
			return null;
		}

	}

	@Override
	public ITask buildTask(String extData) {
		TaskDemo task = new TaskDemo(jedisCommands);

		String[] exts = extData.split(SPLIT);
		task.rule = exts[0];
		task.position = Integer.valueOf(exts[1]);
		task.setExtData(extData);

		return task;
	}

	@Override
	public Queue buildDataQueue(ITask task) {
		TaskDemo taskDemo = (TaskDemo)task;
		 Queue queue= new ArrayBlockingQueue(eachQueueDataSize);
		if(position >= totalQueueDataSize){
			taskDemo.status = DONE_STATUS;
		}else{
			// demon 模拟填充数据
			for(int i = 0; i< eachQueueDataSize; i++){
				Data data = new Data(Math.random());
				queue.offer(data);
			}
			// 纪录上下文位置
			taskDemo.position += eachQueueDataSize;
			taskDemo.status = RUNINNG_STATUS;
		}
		return queue;

	}

	@Override
	public Queue buildEmptyQueue() {
		Queue queue = new ArrayBlockingQueue(1);
		return queue;
	}

	@Override
	public String buildExtData(ITask task) {
		TaskDemo taskDemo = (TaskDemo)task;
		taskDemo.extData = taskDemo.rule+taskDemo.SPLIT+String.valueOf(taskDemo.position);
		return taskDemo.extData;
	}

	@Override
	public boolean updateBatchTaskStatus(String batchId) {
		batchStatus.put(batchId,DONE_STATUS);
		return true;
	}

	@Override
	public boolean isTaskDone(ITask task) {
		TaskDemo taskDemo = (TaskDemo)task;
		return taskDemo.status.equals(DONE_STATUS);
	}

	@Override
	public String getRedisKeyPrefix() {
		return "task:ubas:FromEsToRedis34";
	}
}

class Data{
	private Double value;
	public Data(Double value){
		this.value =value;
	}

	@Override
	public String toString() {
		return "Data{" +
				"value=" + value +
				'}';
	}
}
