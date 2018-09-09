package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedStatus;
import ted.driver.TedResult;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.Registry.TaskConfig;
import ted.driver.sys.TaskManager.TedRunnable;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2018.08.07
 *
 * for internal usage only!!!
 */
class EventQueueManager {
	private static final Logger logger = LoggerFactory.getLogger(EventQueueManager.class);
	private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

	private final TedContext context;
	private final TedDao tedDao;
	private final TedDaoExt tedDaoExt;

	public EventQueueManager(TedContext context) {
		this.context = context;
		this.tedDao = context.tedDao;
		this.tedDaoExt = context.tedDaoExt;
	}

	// channels - always TedEQ.
	// there will be "head" event with status NEW/RETRY, and may be tail events with status SLEEP.
	// heads uniqueness by key1 should be guaranteed by unique index.
	void processTedQueue() {
		int totalProcessing = context.taskManager.calcWaitingTaskCountInAllChannels();
		if (totalProcessing >= TaskManager.LIMIT_TOTAL_WAIT_TASKS) {
			logger.warn("Total size of waiting tasks ({}) already exceeded limit ({}), skip this iteration (2)", totalProcessing, TaskManager.LIMIT_TOTAL_WAIT_TASKS);
			return;
		}
		Channel channel = context.registry.getChannelOrMain(Model.CHANNEL_QUEUE);
		int maxTask = context.taskManager.calcChannelBufferFree(channel);
		maxTask = Math.min(maxTask, 50);
		Map<String, Integer> channelSizes = new HashMap<String, Integer>();
		channelSizes.put(Model.CHANNEL_QUEUE, maxTask);
		List<TaskRec> heads = tedDao.reserveTaskPortion(channelSizes);
		if (heads.isEmpty())
			return;

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
	private void processEventQueue(final TaskRec head) {
		final TedResult headResult = processEvent(head);
		TaskConfig tc = context.registry.getTaskConfig(head.name);
		if (tc == null) {
			context.taskManager.handleUnknownTasks(asList(head));
			return;
		}

		TaskRec lastUnsavedEvent = null;
		TedResult lastUnsavedResult = null;
		// try to execute next events, while head is reserved. some events may be created while executing current
		if (headResult.status == TedStatus.DONE) {
			outer:
			for (int i = 0; i < 10; i++) {
				List<TaskRec> events = tedDaoExt.eventQueueGetTail(head.key1);
				if (events.isEmpty())
					break outer;
				for (TaskRec event : events) {
					TaskConfig tc2 = context.registry.getTaskConfig(event.name);
					if (tc2 == null)
						break outer; // unknown task, leave it for later

					TedResult result = processEvent(event);

					// DONE - final status, on which can continue with next event
					if (result.status == TedStatus.DONE) {
						saveResult(event, result);
					} else {
						lastUnsavedEvent = event;
						lastUnsavedResult = result;
						break outer;
					}
				}
			}
		}

		// first save head, otherwise unique index will fail
		final TedResult finalLastUnsavedResult = lastUnsavedResult;
		final TaskRec finalLastUnsavedEvent = lastUnsavedEvent;
		tedDaoExt.runInTx(new Runnable() {
			@Override
			public void run() {
				try {
					saveResult(head, headResult);
					if (finalLastUnsavedResult != null) {
						saveResult(finalLastUnsavedEvent, finalLastUnsavedResult);
					}
				} catch (Exception e) {
					logger.error("Error while finishing events queue execution", e);
				}
			}
		});
	}


	Long createEvent(String taskName, String queueId, String data, String key2) {
		Long taskId = tedDaoExt.createEvent(taskName, queueId, data, key2);
		tedDaoExt.eventQueueMakeFirst(queueId);
		return taskId;
	}

	Long createEventAndTryExecute(String taskName, String queueId, String data, String key2) {
		long taskId = tedDaoExt.createEvent(taskName, queueId, data, key2);
		TaskRec task = tedDaoExt.eventQueueMakeFirst(queueId);
		if (task != null && task.taskId == taskId) {
			TaskRec taskRec = tedDaoExt.eventQueueReserveTask(task.taskId);
			if (taskRec != null && taskRec.taskId == taskId) {
				processEventQueue(task);
			}
		}
		return taskId;
	}


	private TedResult processEvent(TaskRec taskRec1) {
		String threadName = Thread.currentThread().getName();
		logger.debug("Start to process event {}", taskRec1);
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
					// return as is
				}
			} else if (result.status == TedStatus.DONE || result.status == TedStatus.ERROR) {
				// return as is
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
