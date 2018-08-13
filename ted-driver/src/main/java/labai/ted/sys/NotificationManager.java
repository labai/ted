package labai.ted.sys;

import labai.ted.Ted.TedProcessor;
import labai.ted.sys.Model.TaskRec;
import labai.ted.sys.Registry.Channel;
import labai.ted.sys.Registry.TaskConfig;
import labai.ted.sys.TaskManager.TedRunnable;
import labai.ted.sys.TedDriverImpl.TedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * @author Augustus
 *         created on 2018.07.28
 *
 * for TED internal usage only!!!
 *
 * Notification message can be sent to all active instances (nodes).
 *
 */
class NotificationManager {
	private static final Logger logger = LoggerFactory.getLogger(NotificationManager.class);
	private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

	private static final int KEEP_IN_BUFFER_MS = 2 * 1000;
	private static final int CHECK_IN_DB_LAG_MS = 1 * 1000;

	private final TedContext context;
	private final TedDao tedDao;

	private long lastCheckMs = System.currentTimeMillis() - 20 * 1000;

	// will store notifications for few seconds
	// to filter in case of getting the same notification few times
	private final HashMap<Long, Date> history = new HashMap<Long, Date>();

	public NotificationManager(TedContext context) {
		this.context = context;
		this.tedDao = context.tedDao;
	}


	void processNotifications() {
		Channel systemChannel = context.registry.getChannel(Model.CHANNEL_SYSTEM);
		if (systemChannel == null)
			throw new IllegalStateException("Channel '" + Model.CHANNEL_SYSTEM + "' does not exists, but is required for batch processing");
		long nowMs = System.currentTimeMillis();
		List<TaskRec> notifications = getNewNotifications();
		for (final TaskRec notify : notifications) {
			logger.debug("exec notification for {} {}", notify.name, notify.taskId);
			systemChannel.workers.execute(new TedRunnable(notify) {
				@Override
				public void run() {
					processNotificationTask(notify);
				}
			});
		}

		if (context.prime.isEnabled() == false || context.prime.isPrime()) {
			logger.debug("cleaning notifications");
			long keepInDbMs = context.config.intervalDriverMs() + 1000; // 1000 - reserve
			tedDao.cleanupNotifications(new Date(nowMs - keepInDbMs));
		}
	}


	private void processNotificationTask(TaskRec notify) {
		TaskConfig taskConfig = context.registry.getTaskConfig(notify.name);
		if (taskConfig == null) {
			logger.info("task config does not exists for {} {}", notify.name, notify.taskId);
			return;
		}

		String threadName = Thread.currentThread().getName();
		try {
			Thread.currentThread().setName(threadName + "-" + taskConfig.shortLogName + "-" + notify.taskId);
			TedProcessor processor = taskConfig.tedProcessorFactory.getProcessor(notify.name);
			processor.process(notify.getTedTask());
		} catch (Exception e) {
			logger.error("Unhandled exception while calling notification processor for task '{}': {}", notify.name, e.getMessage());
		} finally {
			Thread.currentThread().setName(threadName);
		}
		return;

	}


	private List<TaskRec> getNewNotifications() {
		List<TaskRec> newrecs = new ArrayList<TaskRec>();
		synchronized (this) {
			long currTs = System.currentTimeMillis();
			List<TaskRec> recs = tedDao.getLastNotifications(new Date(lastCheckMs - CHECK_IN_DB_LAG_MS));
			lastCheckMs = currTs;
			// remove from result those, what already have in history
			for (TaskRec rec : recs) {
				if (history.containsKey(rec.taskId))
					continue;
				newrecs.add(rec);
			}
			// remove oldest from history
			long now = System.currentTimeMillis();
			for (Iterator<Long> iterator = history.keySet().iterator(); iterator.hasNext();) {
				Long taskId = iterator.next();
				Date dt = history.get(taskId);
				if (now - dt.getTime() > KEEP_IN_BUFFER_MS)
					iterator.remove();
			}
			// add new to history
			for (TaskRec rec : newrecs)
				history.put(rec.taskId, rec.createTs);
		}
		return newrecs;
	}

	Long sendNotification(String taskName, String data) {
		return tedDao.createTask(taskName, Model.CHANNEL_NOTIFY, data, null, null, null);
	}

}

