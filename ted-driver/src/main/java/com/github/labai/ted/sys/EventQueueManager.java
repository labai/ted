package com.github.labai.ted.sys;

import com.github.labai.ted.Ted.TedProcessor;
import com.github.labai.ted.Ted.TedResult;
import com.github.labai.ted.Ted.TedStatus;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.Registry.Channel;
import com.github.labai.ted.sys.Registry.TaskConfig;
import com.github.labai.ted.sys.TaskManager.TedRunnable;
import com.github.labai.ted.sys.TedDriverImpl.TedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EventQueueManager {
	private static final Logger logger = LoggerFactory.getLogger(EventQueueManager.class);
	private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

	private TedContext context;
	private TedDao tedDao;

	public EventQueueManager(TedContext context) {
		this.context = context;
		this.tedDao = context.tedDao;
	}

	// channels - always QUEUE.
	// there will be "head" event with status NEW/RETRY, and may be tail events with status SLEEP.
	// heads uniqueness by key1 should be guaranteed by unique index.
	void processTedQueue() {
		int totalProcessing = context.taskManager.calcWaitingTaskCountInAllChannels();
		if (totalProcessing >= TaskManager.LIMIT_TOTAL_WAIT_TASKS) {
			logger.warn("Total size of waiting tasks ({}) already exceeded limit ({}), skip this iteration (2)", totalProcessing, TaskManager.LIMIT_TOTAL_WAIT_TASKS);
			return;
		}
		Channel channel = context.registry.getChannel(Model.CHANNEL_QUEUE);
		if (channel == null)
			throw new IllegalStateException("Channel '" + Model.CHANNEL_QUEUE + "' does not exists, but is required for event queue processing");
		int maxTask = context.taskManager.calcChannelBufferFree(channel);
		maxTask = Math.min(maxTask, 50);
		Map<String, Integer> channelSizes = new HashMap<String, Integer>();
		channelSizes.put(Model.CHANNEL_QUEUE, maxTask);
		List<TaskRec> heads = tedDao.reserveTaskPortion(channelSizes);
		if (heads.isEmpty())
			return;
		List<String> discriminators = new ArrayList<String>();

		for (TaskRec task : heads) {
			discriminators.add(task.key1);
		}

		// add tails to heads to make event queues
//		List<TaskRec> tails = tedDao.eventQueueGetTail(discriminators);
//		Map<String, List<TaskRec>> queueMap = new HashMap<String, List<TaskRec>>(heads.size());
//		for (TaskRec head : heads) {
//			List<TaskRec> eventQueue = new ArrayList<TaskRec>();
//			eventQueue.add(head);
//			queueMap.put(head.key1, eventQueue);
//		}
//		for (TaskRec tail : tails) {
//			queueMap.get(tail.key1).add(tail);
//		}

		// send to workers
//		for (final String discriminator : queueMap.keySet()) {
//			logger.debug("exec eventQueue for '{}'", discriminator);
//			final List<TaskRec> events = queueMap.get(discriminator);
//			channel.workers.execute(new TedRunnable(events) {
//				@Override
//				public void run() {
//					processEventQueue(events);
//				}
//			});
//		}
		for (final TaskRec head : heads) {
			logger.debug("exec eventQueue for '{}', headTaskId={}", head.key1, head.taskId);
			channel.workers.execute(new TedRunnable(head) {
				@Override
				public void run() {
					processEventQueue(head);
				}
			});
		}
	}

	private void saveResult(TaskRec event, TedResult result) {
		if (result.status == TedStatus.RETRY) {
			TaskConfig tc = context.registry.getTaskConfig(event.name);
			Date nextTm = tc.retryScheduler.getNextRetryTime(event.getTedTask(), event.retries + 1, event.startTs);
			tedDao.setStatusPostponed(event.taskId, result.status, result.message, nextTm);
		} else {
			tedDao.setStatus(event.taskId, result.status, result.message);
		}
	}

	// process events from queue each after other, until ERROR or RETRY will happen
	void processEventQueue(TaskRec head) {
		TedResult headResult = processEvent(head);
		TaskRec lastUnsavedEvent = null;
		TedResult lastUnsaved = null;
		// try to execute next events, while head is reserved. some events may be created while executing current
		if (headResult.status == TedStatus.DONE) {
			outer:
			for (int i = 0; i < 10; i++) {
				List<TaskRec> events = tedDao.eventQueueGetTail(head.key1);
				if (events.isEmpty())
					break;
				for (TaskRec event : events) {
					TedResult result = processEvent(event);

					// DONE - final status, on which can continue with next event
					if (result.status == TedStatus.DONE) {
						saveResult(event, result);
					} else {
						lastUnsavedEvent = event;
						lastUnsaved = result;
						break outer;
					}
				}
			}
		}
		saveResult(head, headResult);
		if (lastUnsaved != null) {
			saveResult(lastUnsavedEvent, lastUnsaved);
		}
	}

	Long createEvent(String taskName, String discriminator, String data, String key2) {
		Long taskId = tedDao.createEvent(taskName, discriminator, data, key2);
		tedDao.eventQueueMakeFirst(discriminator);
		return taskId;
	}
	Long createAndTryExecuteEvent(String taskName, String discriminator, String data, String key2) {
		Long taskId = tedDao.createEvent(taskName, discriminator, data, key2);
		TaskRec task = tedDao.eventQueueMakeFirst(discriminator);
		if (task != null && task.taskId == (long) taskId) {
			processEventQueue(task);
		}
		return taskId;
	}


	private TedResult processEvent(TaskRec taskRec1) {
		TaskConfig tc = context.registry.getTaskConfig(taskRec1.name);
		if (tc == null) {
			logger.error("no processor exists for event task={}", taskRec1.name);
			return TedResult.error("no processor exists"); // TODO handle unknown tasks
		}
		String threadName = Thread.currentThread().getName();
		TedResult result;
		try {
			TaskConfig taskConfig = context.registry.getTaskConfig(taskRec1.name);
			Thread.currentThread().setName(threadName + "-" + taskConfig.shortLogName + "-" + taskRec1.taskId);

			// process
			//
			TedProcessor processor = taskConfig.tedProcessorFactory.getProcessor(taskRec1.name);
			result = processor.process(taskRec1.getTedTask());

			// check results
			//
			if (result == null) {
				result = TedResult.error("result is null");
			} else if (result.status == TedStatus.RETRY) {
				Date nextTm = taskConfig.retryScheduler.getNextRetryTime(taskRec1.getTedTask(), taskRec1.retries + 1, taskRec1.startTs);
				if (nextTm == null) {
					result = TedResult.error("max retries. " + result.message);
				} else {
					//tedDao.setStatusPostponed(trec.taskId, result.status, result.message, nextTm);
				}
			} else if (result.status == TedStatus.DONE || result.status == TedStatus.ERROR) {

			} else {
				result = TedResult.error("invalid result status: " + result.status);
			}

		} catch (Exception e) {
			logger.info("Unhandled exception while calling processor for task '{}': {}", taskRec1.name, e.getMessage());
			taskExceptionLogger.error("Unhandled exception while calling processor for task '" + taskRec1.name + "'", e);
			result = TedResult.error("Catch: " + e.getMessage());
		} finally {
			Thread.currentThread().setName(threadName);
		}
		return result;
	}

}
