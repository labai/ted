package labai.ted.sys;

import labai.ted.Ted.TedStatus;
import labai.ted.sys.Model.TaskRec;
import labai.ted.sys.Registry.Channel;
import labai.ted.sys.Registry.TaskConfig;
import labai.ted.sys.TaskManager.TedRunnable;
import labai.ted.sys.TedDriverImpl.TedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2018.08.09
 *
 * for TED internal usage only!!!
 *
 */
class BatchWaitManager {
	private static final Logger logger = LoggerFactory.getLogger(BatchWaitManager.class);
	private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

	private final TedContext context;
	private final TedDao tedDao;

	public BatchWaitManager(TedContext context) {
		this.context = context;
		this.tedDao = context.tedDao;
	}

	// will check, are all subtasks finished (DONE or ERROR)
	// if finished, then move this batchTask to his channel and then will be processed as regular task
	void processBatchWaitTasks() {
		// channel in db - TedBW, but will use TedSS threadPool - less different threads
		Channel systemChannel = context.registry.getChannel(Model.CHANNEL_SYSTEM);
		if (systemChannel == null)
			throw new IllegalStateException("Channel '" + Model.CHANNEL_SYSTEM + "' does not exists, but is required for batch processing");

		int maxTask = context.taskManager.calcChannelBufferFree(systemChannel);
		Map<String, Integer> channelSizes = new HashMap<String, Integer>();
		channelSizes.put(Model.CHANNEL_BATCH, maxTask);
		List<TaskRec> batches = context.tedDao.reserveTaskPortion(channelSizes);
		if (batches.isEmpty())
			return;

		for (final TaskRec batchTask : batches) {
			systemChannel.workers.execute(new TedRunnable(batchTask) {
				@Override
				public void run() {
					processBatchWaitTask(batchTask);
				}
			});
		}
	}

	private void processBatchWaitTask(TaskRec batch) {
		TaskConfig tc = context.registry.getTaskConfig(batch.name);
		if (tc == null) {
			context.taskManager.handleUnknownTasks(asList(batch));
			return;
		}

		// check for finishing all tasks before sending it to consumer
		boolean finished = tedDao.checkIsBatchFinished(batch.taskId);
		if (finished) {
			// cleanup retries - then it could be used for task purposes
			logger.debug("Batch {} waiting finished, changing channel to {} and status to NEW", batch.taskId, tc.channel);
			tedDao.cleanupBatchTask(batch.taskId, "", tc.channel);
			tedDao.setStatusPostponed(batch.taskId, TedStatus.NEW, "", new Date());
		}

		// retry batch
		long batchTimeMn = (System.currentTimeMillis() - batch.createTs.getTime()) / 1000 / 60;
		if (batchTimeMn >= tc.batchTimeoutMinutes) {
			logger.warn("Batch timeout for task_id=" + batch.taskId + " name=" + batch.name + " createTs=" + batch.createTs+ " now=" + MiscUtils.dateToStrTs(System.currentTimeMillis()) + " ttl-minutes=" + tc.batchTimeoutMinutes);
			tedDao.setStatus(batch.taskId, TedStatus.ERROR, "Batch processing too long");
			return;
		}
		Date nextTm = ConfigUtils.BATCH_RETRY_SCHEDULER.getNextRetryTime(batch.getTedTask(), batch.retries + 1, batch.startTs);
		if (nextTm == null)
			nextTm = new Date(System.currentTimeMillis() + 60 * 1000);
		tedDao.setStatusPostponed(batch.taskId, TedStatus.RETRY, Model.BATCH_MSG, nextTm);
	}

}
