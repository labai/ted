package com.github.labai.ted.sys;


import com.github.labai.ted.Ted.TedPackProcessor;
import com.github.labai.ted.Ted.TedProcessor;
import com.github.labai.ted.Ted.TedResult;
import com.github.labai.ted.Ted.TedStatus;
import com.github.labai.ted.Ted.TedTask;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.Registry.Channel;
import com.github.labai.ted.sys.Registry.TaskConfig;
import com.github.labai.ted.sys.TedDriverImpl.TedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * @author Augustus
 *         created on 2016.09.19
 *
 * for internal usage only!!!
 */
class TaskManager {
	private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);
	private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

	static final int SLOW_START_COUNT = 3;
	static final int MAX_TASK_COUNT = 1000;
	static final int LIMIT_TOTAL_WAIT_TASKS = 20000; // max waiting tasks (aim to don't consume all memory)
	private static final long RARE_MAINT_INTERVAL_MILIS = 3 * 3600 * 1000; // every 3 hour
	private static final long UNKNOWN_TASK_POSTPONE_MS = 120 * 1000; // 2 min
	private static final long UNKNOWN_TASK_CANCEL_AFTER_MS = 24 * 3600 * 1000;

	private final TedContext context;

	private class ChannelWorkContext {
		final String channelName;
		int nextSlowLimit = SLOW_START_COUNT;
		int lastGotCount = 0;
		int nextPortion = 0;
		boolean foundTask = false;
		ChannelWorkContext(String channelName) {
			this.channelName = channelName;
			dropNextSlowLimit();
		}
		void dropNextSlowLimit() {
			this.nextSlowLimit = context.registry.getChannel(channelName).getSlowStartCount();
		}
	}
	private Map<String, ChannelWorkContext> channelContextMap = new HashMap<String, ChannelWorkContext>();



	private long lastRareMaintExecTimeMilis = 0;

	abstract static class TedRunnable implements Runnable {
		private final TaskRec task;
		private final List<TaskRec> tasks;
		public TedRunnable(TaskRec task) {
			this.task = task;
			this.tasks = null;
		}
		public TedRunnable(List<TaskRec> tasks) {
			this.task = null;
			this.tasks = new ArrayList<TaskRec>(tasks);
		}
		public List<TaskRec> getTasks() {
			if (tasks != null)
				return tasks;
			return Collections.singletonList(task);
		}
		public int getTaskCount() {
			return (tasks != null ? tasks.size() : 1);
		}
	}

// TODO
//	private static class SetStatus {
//		final long taskId;
//		final TedStatus status;
//		final String msg;
//		final Date nextTs;
//		public SetStatus(long taskId, TedStatus status, String msg, Date nextTs) {
//			this.taskId = taskId;
//			this.status = status;
//			this.msg = msg;
//			this.nextTs = nextTs;
//		}
//	}
//
//	private ConcurrentLinkedQueue<SetStatus> statusQueue = new ConcurrentLinkedQueue<SetStatus>();

	TaskManager(TedContext context) {
		this.context = context;
	}

	public void changeTaskStatusPostponed(long taskId, TedStatus status, String msg, Date nextTs){
		// TODO
		//statusQueue.add(new SetStatus(taskId, status, msg, nextTs));
		context.tedDao.setStatusPostponed(taskId, status, msg, nextTs);

	}
	public void changeTaskStatus(long taskId, TedStatus status, String msg){
		changeTaskStatusPostponed(taskId, status, msg, null);
	}

	// process maintenance tasks
	//
	void processMaintenanceTasks() {
		if (context.prime.isEnabled()) {
			if (! context.prime.isPrime()) {
				logger.debug("Skip ted-maint as is not prime instance");
				return;
			}
		}
		context.tedDao.processMaintenanceFrequent();
		processTimeouts();
		if (System.currentTimeMillis() - lastRareMaintExecTimeMilis > RARE_MAINT_INTERVAL_MILIS) {
			logger.debug("Start process rare maintenance tasks");
			context.tedDao.processMaintenanceRare(context.config.oldTaskArchiveDays());
			lastRareMaintExecTimeMilis = System.currentTimeMillis();
		}
	}

	private void processTimeouts() {
		List<TaskRec> tasks = context.tedDao.getWorkingTooLong();
		long nowTime = System.currentTimeMillis();
		for (TaskRec task: tasks) {
			long workingTimeMn = (nowTime - task.startTs.getTime()) / 1000 / 60;
			TaskConfig tc = context.registry.getTaskConfig(task.name);
			if (tc == null) {
				logger.error("Unknown task " + task);
				continue;
			}
			if (tc.workTimeoutMinutes <= workingTimeMn) {
				if (logger.isDebugEnabled())
					logger.debug("Work timeout for task_id=" + task.taskId + " name=" + task.name + " startTs=" + task.startTs+ " now=" + dateToStrTs(nowTime) + " ttl-minutes=" + tc.workTimeoutMinutes);
				changeTaskStatusPostponed(task.taskId, TedStatus.RETRY, Model.TIMEOUT_MSG + "(3)", new Date());
			} else {
				if (logger.isDebugEnabled())
					logger.debug("Set finishTs for task_id=" + task.taskId + " name=" + task.name + " startTs=" + task.startTs+ " now=" + dateToStrTs(nowTime) + " ttl-minutes=" + tc.workTimeoutMinutes);
				context.tedDao.setTaskPlannedWorkTimeout(task.taskId, new Date(task.startTs.getTime() + tc.workTimeoutMinutes * 60 * 1000));
			}
		}
	}

// TODO
//	void submitStatuses() {
//		List<SetStatus> statuses = new ArrayList<SetStatus>();
//		for (int i = 0; i < 999; i++) {
//			SetStatus ss = statusQueue.poll();
//			if (ss == null) break;
//			statuses.add(ss);
//		}
//		if (statuses.isEmpty())
//			return;
//		context.tedDao.submitStatuses(statuses);
//	}

	// process TED tasks
	//
	void processChannelTasks() {
		List<String> waitChannelsList = context.tedDao.getWaitChannels();
		waitChannelsList.removeAll(Model.nonTaskChannels);
		processChannelTasks(waitChannelsList);
	}

	void processChannelTasks(List<String> waitChannelsList) {
		int totalProcessing = calcWaitingTaskCountInAllChannels();
		if (totalProcessing >= LIMIT_TOTAL_WAIT_TASKS) {
			logger.warn("Total size of waiting tasks ({}) already exceeded limit ({}), skip this iteration", totalProcessing, LIMIT_TOTAL_WAIT_TASKS);
			return;
		}

		// List<String> waitChannelsList = context.tedDao.getWaitChannels();
		if (waitChannelsList.isEmpty()) {
			logger.trace("no wait tasks");
			for (ChannelWorkContext wc : channelContextMap.values())
				wc.dropNextSlowLimit();
			return;
		}

		// check and log for unknown channels, remove special channels

		for (String waitChan : waitChannelsList) {
			if (context.registry.getChannel(waitChan) == null)
				logger.warn("Channel '" + waitChan + "' is not configured, but exists a waiting task with that channel");
		}

		Collection<Channel> channels = context.registry.getChannels();
		for (Channel channel : channels) {
			ChannelWorkContext wc = channelContextMap.get(channel.name);
			if (wc == null) {
				wc = new ChannelWorkContext(channel.name);
				channelContextMap.put(channel.name, wc);
			}
			wc.foundTask = waitChannelsList.contains(channel.name);
			if (!wc.foundTask)
				continue;
			// calc max size of tasks we can take now
			int maxTask = calcChannelBufferFree(channel);
			maxTask = Math.min(maxTask, wc.nextSlowLimit); // limit maximums by iteration(slow start)
			wc.nextPortion = maxTask;
		}

		// fill channels for request
		Map<String, Integer> channelSizes = new HashMap<String, Integer>();
		for (ChannelWorkContext wc : channelContextMap.values()) {
			if (wc.nextPortion > 0 && wc.foundTask)
				channelSizes.put(wc.channelName, wc.nextPortion);
		}

		// select tasks from db
		//
		List<TaskRec> tasks = context.tedDao.reserveTaskPortion(channelSizes);

		// calc stats
		//
		for (ChannelWorkContext wc : channelContextMap.values()) {
			wc.lastGotCount = 0;
		}
		for (TaskRec taskRec : tasks) {
			ChannelWorkContext wc = channelContextMap.get(taskRec.channel);
			if (wc == null) {
				wc = new ChannelWorkContext(taskRec.channel);
				channelContextMap.put(taskRec.channel, wc);
			}
			wc.lastGotCount++;
		}
		// update next portion - double if found something, clear to minimum if not
		for (ChannelWorkContext wc : channelContextMap.values()) {
			if (wc.foundTask) {
				wc.nextSlowLimit = Math.min(wc.nextSlowLimit * 2, MAX_TASK_COUNT);
				logger.debug("Channel " + wc.channelName + " nextSlowLimit=" + wc.nextSlowLimit);
			} else {
				wc.dropNextSlowLimit();
			}
		}

		if (tasks.isEmpty()) {
			logger.debug("no tasks (full check)");
			return;
		}

		sendTaskListToChannels(tasks);
	}


	// will send task to their channels for execution
	void sendTaskListToChannels(List<TaskRec> tasks) {
		// group by type
		//
		Map<String, List<TaskRec>> grouped = new HashMap<String, List<TaskRec>>();
		for (TaskRec taskRec : tasks) {
			List<TaskRec> tlist = grouped.get(taskRec.name);
			if (tlist == null) {
				tlist = new ArrayList<TaskRec>();
				grouped.put(taskRec.name, tlist);
			}
			tlist.add(taskRec);
		}

		// execute
		//
		for (String taskName : grouped.keySet()) {
			final List<TaskRec> taskList = grouped.get(taskName);
			TaskRec trec1 = taskList.get(0);
			Channel channel = context.registry.getChannel(trec1.channel);
			if (channel == null) { // should never happen
				logger.warn("Task channel '" + trec1.channel + "' not exists. Use channel MAIN (task={} taskId={})", trec1.name, trec1.taskId);
				channel = context.registry.getChannel(Model.CHANNEL_MAIN);
			}
			TaskConfig taskConfig = context.registry.getTaskConfig(trec1.name);
			if (taskConfig == null) {
				handleUnknownTasks(taskList);
				continue;
			}
			if (taskConfig.isPackProcessing) {
				logger.debug("got tasks (task={} count={}) for pack processing", trec1.name, taskList.size());
				channel.workers.execute(new TedRunnable(taskList) {
					@Override
					public void run() {
						processTask(taskList);
					}
				});
			} else {
				for (final TaskRec taskRec : taskList) {
					logger.debug("got task: " + taskRec);
					channel.workers.execute(new TedRunnable(taskRec) {
						@Override
						public void run() {
							processTask(Collections.singletonList(taskRec));
						}
					});
				}
			}
		}
	}

	private void handleUnknownTasks(List<TaskRec> taskRecList) {
		long nowMs = System.currentTimeMillis();
		for (TaskRec taskRec : taskRecList) {
			if (taskRec.createTs.getTime() < nowMs - UNKNOWN_TASK_CANCEL_AFTER_MS) {
				logger.warn("Task is unknown and was not processed during 24 hours, mark as error: {}", taskRec);
				changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "unknown task");
			} else {
				logger.warn("Task is unknown, mark as new, postpone: {}", taskRec);
				changeTaskStatusPostponed(taskRec.taskId, TedStatus.NEW, "unknown task. postpone", new Date(nowMs + UNKNOWN_TASK_POSTPONE_MS));
			}
		}
	}

	// Remarks:
	// - all tasks must be of same type (will not be checked)
	//
//	TedResult processTask(TaskRec taskRec) {
//		Map<Long, TedResult> results = processTask(Collections.singletonList(taskRec));
//		TedResult result = results.get(taskRec.taskId);
//		if (result == null)
//			result = TedResult.error("Result was lost");
//		return result;
//	}

	Map<Long, TedResult> processTask(List<TaskRec> taskRecList) {
		if (taskRecList == null || taskRecList.isEmpty())
			throw new IllegalStateException("taskRecList is empty");
		TaskRec taskRec1 = taskRecList.get(0);

		TedDao tedDao = context.tedDao;
		String threadName = Thread.currentThread().getName();
		Map<Long, TedResult> results = new HashMap<Long, TedResult>();
		try {

			TaskConfig taskConfig = context.registry.getTaskConfig(taskRec1.name);

			// check if batch
			if (Model.BATCH_MSG.equals(taskRec1.msg)) {
				// check for finishing all tasks before sending it to consumer
				boolean finished = tedDao.checkIsBatchFinished(taskRec1.taskId);
				//String msg1 = "waiting for finish... [B" + taskRec1.taskId + "]";
				if (!finished) {
					// retry batch
					long batchTimeMn = (System.currentTimeMillis() - taskRec1.createTs.getTime()) / 1000 / 60;
					if (batchTimeMn >= taskConfig.batchTimeoutMinutes) {
						logger.warn("Batch timeout for task_id=" + taskRec1.taskId + " name=" + taskRec1.name + " createTs=" + taskRec1.createTs+ " now=" + dateToStrTs(System.currentTimeMillis()) + " ttl-minutes=" + taskConfig.batchTimeoutMinutes);
						changeTaskStatus(taskRec1.taskId, TedStatus.ERROR, "Batch processing too long");
						return Collections.emptyMap();
					}
					Date nextTm = ConfigUtils.BATCH_RETRY_SCHEDULER.getNextRetryTime(taskRec1.getTedTask(), taskRec1.retries + 1, taskRec1.startTs);
					if (nextTm == null)
						nextTm = new Date(System.currentTimeMillis() + 60 * 1000);
					tedDao.setStatusPostponed(taskRec1.taskId, TedStatus.RETRY, Model.BATCH_MSG, nextTm);
					return Collections.emptyMap();
				} else {
					// cleanup retries - then it could be used for task purposes
					tedDao.cleanupRetries(taskRec1.taskId, "");
					taskRec1.retries = 0;
					taskRec1.msg = "";
				}
			}

			Thread.currentThread().setName(threadName + "-" + taskConfig.shortLogName + "-" + taskRec1.taskId);

			// process
			//

			if (taskConfig.isPackProcessing) {
				TedPackProcessor processor = taskConfig.tedPackProcessorFactory.getPackProcessor(taskRec1.name);
				List<TedTask> taskList = new ArrayList<TedTask>();
				for (TaskRec taskRec : taskRecList)
					taskList.add(taskRec.getTedTask());
				results = processor.process(taskList);
				if (results == null)
					results = Collections.emptyMap();
			} else {
				if (taskRecList.size() != 1)
					throw new IllegalStateException("taskRecList size must by 1");
				TedProcessor processor = taskConfig.tedProcessorFactory.getProcessor(taskRec1.name);
				TedResult result1 = processor.process(taskRec1.getTedTask());
				results.put(taskRec1.taskId, result1);
			}

			// check results
			//
			for (TaskRec trec : taskRecList) {
				TedResult result = results.get(trec.taskId);
				if (result == null) {
					changeTaskStatus(trec.taskId, TedStatus.ERROR, "result is null");
				} else if (result.status == TedStatus.RETRY) {
					Date nextTm = taskConfig.retryScheduler.getNextRetryTime(trec.getTedTask(), trec.retries + 1, trec.startTs);
					if (nextTm == null) {
						changeTaskStatus(trec.taskId, TedStatus.ERROR, "max retries. " + result.message);
					} else {
						tedDao.setStatusPostponed(trec.taskId, result.status, result.message, nextTm);
					}
				} else if (result.status == TedStatus.DONE || result.status == TedStatus.ERROR) {
					changeTaskStatus(trec.taskId, result.status, result.message);
				} else {
					changeTaskStatus(trec.taskId, TedStatus.ERROR, "invalid result status: " + result.status);
				}
			}


		} catch (Exception e) {
			logger.info("Unhandled exception while calling processor for task '{}': {}", taskRec1.name, e.getMessage());
			taskExceptionLogger.error("Unhandled exception while calling processor for task '" + taskRec1.name + "'", e);
			try {
				TedResult resultError = TedResult.error("Catch: " + e.getMessage());
				for (TaskRec taskRec : taskRecList) {
					results.put(taskRec.taskId, resultError);
					changeTaskStatus(taskRec.taskId, TedStatus.ERROR, resultError.message);
				}
			} catch (Exception e1) {
				logger.warn("Unhandled exception while handling exception for task '{}', statuses will be not changed: {}", taskRec1.name, e1.getMessage());
			}
		} finally {
			Thread.currentThread().setName(threadName);
		}
		return results;
	}

	int calcChannelBufferFree(Channel channel) {
		int workerCount = channel.workers.getMaximumPoolSize();
		int queueRemain = channel.getQueueRemainingCapacity();
		int maxTask = workerCount - channel.workers.getActiveCount() + queueRemain;
		maxTask = Math.max(Math.min(maxTask, MAX_TASK_COUNT), 0);
		logger.debug(channel.name + " max_count=" + maxTask + " (workerCount=" + workerCount + " activeCount=" + channel.workers.getActiveCount() + " remainingCapacity=" + queueRemain + " maxQueue=" + channel.taskBufferSize + ")");
		if (maxTask == 0) {
			logger.debug("Channel " + channel.name + " queue is full");
		}
		return maxTask;
	}


	int calcWaitingTaskCountInAllChannels() {
		int sum = 0;
		for (Channel channel : context.registry.getChannels()) {
			Queue<Runnable> queue = channel.workers.getQueue();
			try {
				for (Runnable r : queue) {
					TedRunnable tr = (TedRunnable) r;
					sum = sum + tr.getTaskCount();
				}
			} catch (ConcurrentModificationException e) {
				// should not happen as LinkedBlockingQueue should not raise this exception
				continue;
			}
		}
		return sum;
	}

	// for logging
	private static String dateToStrTs(long dateMs) {
		SimpleDateFormat df = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSS");
		return df.format(new Date(dateMs));
	}
}
