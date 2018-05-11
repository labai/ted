package com.github.ted.sys;


import com.github.ted.Ted.TedProcessor;
import com.github.ted.Ted.TedResult;
import com.github.ted.Ted.TedStatus;
import com.github.ted.sys.Model.TaskRec;
import com.github.ted.sys.Registry.Channel;
import com.github.ted.sys.Registry.TaskConfig;
import com.github.ted.sys.Registry.TaskType;
import com.github.ted.sys.TedDriverImpl.TedContext;
import com.github.ted.sys.TedDriverImpl.TedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Augustus
 *         created on 2016.09.19
 *
 * for internal usage only!!!
 */
class TaskManager {
	private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);
	private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

	private static final int START_TASK_COUNT = 3;
	private static final int MAX_TASK_COUNT = 999;
	private static final long RARE_MAINT_INTERVAL_MILIS = 3 * 3600 * 1000; // every 3 hour
	private static final long UNKNOWN_TASK_POSTPONE_MS = 120 * 1000; // 2 min
	private static final long UNKNOWN_TASK_CANCEL_AFTER_MS = 24 * 3600 * 1000;

	private final TedContext context;

	private static class ChannelWorkContext {
		final String taskName;
		int nextSlowLimit = START_TASK_COUNT;
		int lastGotCount = 0;
		int nextPortion = 0;
		boolean foundTask = false;
		ChannelWorkContext(String taskName) {
			this.taskName = taskName;
		}
	}
	private Map<String, ChannelWorkContext> channelContextMap = new HashMap<String, ChannelWorkContext>();

	private long lastRareMaintExecTimeMilis = 0;

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
				changeTaskStatusPostponed(task.taskId, TedStatus.RETRY, "Too long in status [work](3)", new Date());
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
	void processTasks() {

		List<String> waitChannelsList = context.tedDao.getWaitChannels();
		if (waitChannelsList.isEmpty()) {
			logger.trace("no wait tasks");
			for (ChannelWorkContext wc : channelContextMap.values())
				wc.nextSlowLimit = START_TASK_COUNT;
			return;
		}

		// check and log for unknown channels
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
			int workerCount = channel.workers.getMaximumPoolSize();
			int queueRemain = Math.min(channel.workers.getQueue().remainingCapacity() - channel.workerCount, MAX_TASK_COUNT);
			int maxTask = workerCount - channel.workers.getActiveCount() + queueRemain;
			maxTask = Math.max(Math.min(maxTask, MAX_TASK_COUNT), 0);
			logger.debug(channel.name + " max_count=" + maxTask + " (workerCount=" + workerCount + " activeCount=" + channel.workers.getActiveCount() + " remainingCapacity=" + queueRemain + " maxQueue=" + channel.taskBufferSize + ")");
			if (maxTask == 0) {
				logger.debug("Channel " + channel.name + " queue is full");
			}
			maxTask = Math.min(maxTask, wc.nextSlowLimit); // limit maximums by iteration(slow start)
			wc.nextPortion = maxTask;
		}

		// fill channels for request
		Map<String, Integer> channelSizes = new HashMap<String, Integer>();
		for (ChannelWorkContext wc : channelContextMap.values()) {
			if (wc.nextPortion > 0 && wc.foundTask)
				channelSizes.put(wc.taskName, wc.nextPortion);
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
				logger.debug("Channel " + wc.taskName + " nextSlowLimit=" + wc.nextSlowLimit);
			} else {
				wc.nextSlowLimit = START_TASK_COUNT;
			}
		}

		if (tasks.isEmpty()) {
			logger.debug("no tasks (full check)");
			return;
		}

		// execute tasks
		//
		for (final TaskRec taskRec : tasks) {
			Channel channel = context.registry.getChannel(taskRec.channel);
			if (channel == null) { // should never happen
				logger.error("Task channel '" + taskRec.channel + "' not exists. Set task " + taskRec.name + " taskId=" + taskRec.taskId + " status to ERROR");
				changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "invalid channel");
				continue;
			}
			logger.debug("got task: " + taskRec);
			channel.workers.execute(new TedRunnable(taskRec) {
				@Override
				public void run() {
					processTask(taskRec);
				}
			});
		}
	}

	private void processTask(TaskRec taskRec) {
		TedDao tedDao = context.tedDao;
		String threadName = Thread.currentThread().getName();
		try {
			TaskConfig taskConfig = context.registry.getTaskConfig(taskRec.name);
			if (taskConfig == null) {
				long nowMs = System.currentTimeMillis();
				if (taskRec.createTs.getTime() < nowMs - UNKNOWN_TASK_CANCEL_AFTER_MS) {
					logger.warn("Task is unknown and was not processed during 24 hours, mark as error: {}", taskRec);
					changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "unknown task");
				} else {
					logger.warn("Task is unknown, mark as new, postpone: {}", taskRec);
					changeTaskStatusPostponed(taskRec.taskId, TedStatus.NEW, "unknown task. postpone", new Date(nowMs + UNKNOWN_TASK_POSTPONE_MS));
				}
				return;
			}

			// check if batch
			if (taskConfig.taskType == TaskType.BATCH) {
				// check for finishing all tasks before sending it to consumer
				boolean finished = tedDao.checkIsBatchFinished(taskRec.taskId);
				String msg1 = "waiting for finish... [B" + taskRec.taskId + "]";
				if (!finished) {
					// retry batch
					long batchTimeMn = (System.currentTimeMillis() - taskRec.createTs.getTime()) / 1000 / 60;
					if (batchTimeMn >= taskConfig.batchTimeoutMinutes) {
						logger.warn("Batch timeout for task_id=" + taskRec.taskId + " name=" + taskRec.name + " createTs=" + taskRec.createTs+ " now=" + dateToStrTs(System.currentTimeMillis()) + " ttl-minutes=" + taskConfig.batchTimeoutMinutes);
						changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "Batch processing too long");
						return;
					}
					Date nextTm = ConfigUtils.BATCH_RETRY_SCHEDULER.getNextRetryTime(taskRec.getTedTask(), taskRec.retries + 1, taskRec.startTs);
					if (nextTm == null)
						nextTm = new Date(System.currentTimeMillis() + 60 * 1000);
					tedDao.setStatusPostponed(taskRec.taskId, TedStatus.RETRY, msg1, nextTm);
					return;
				} else {
					// cleanup retries - then it could be used for task purposes
					if (msg1.equals(taskRec.msg)) {
						tedDao.cleanupRetries(taskRec.taskId, "");
						taskRec.retries = 0;
						taskRec.msg = "";
					}
				}
			}

			TedProcessor processor = taskConfig.tedProcessorFactory.getProcessor(taskRec.name);

			Thread.currentThread().setName(threadName + "-" + taskConfig.shortLogName + "-" + taskRec.taskId);

			// process
			//
			TedResult result = processor.process(taskRec.getTedTask());

			// check result
			//
			if (result == null) {
				changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "result is null");
			} else if (result.status == TedStatus.RETRY) {
				Date nextTm = taskConfig.retryScheduler.getNextRetryTime(taskRec.getTedTask(), taskRec.retries + 1, taskRec.startTs);
				if (nextTm == null) {
					changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "max retries. " + result.message);
				} else {
					tedDao.setStatusPostponed(taskRec.taskId, result.status, result.message, nextTm);
				}
			} else if (result.status == TedStatus.DONE || result.status == TedStatus.ERROR) {
				changeTaskStatus(taskRec.taskId, result.status, result.message);
			} else {
				changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "invalid result status: " + result.status);
			}
		} catch (Exception e) {
			logger.info("Unhandled exception while calling processor for task '{}': {}", taskRec.name, e.getMessage());
			taskExceptionLogger.error("Unhandled exception while calling processor for task '" + taskRec.name + "'", e);
			changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "Catch: " + e.getMessage());
		} finally {
			Thread.currentThread().setName(threadName);
		}

	}


	// for logging
	private static String dateToStrTs(long dateMs) {
		SimpleDateFormat df = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSS");
		return df.format(new Date(dateMs));
	}
}
