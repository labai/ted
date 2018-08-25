package ted.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedRetryScheduler;
import ted.driver.Ted.TedStatus;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys._TedSchdDriverExt;
import ted.scheduler.TedScheduler.TedSchedulerNextTime;
import ted.scheduler.utils.CronExpression;

import javax.sql.DataSource;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Augustus
 *         created on 2018.08.16
 *
 *  for TED internal usage only!!!
 *
 *
 */
class TedSchedulerImpl {
	private static final Logger logger = LoggerFactory.getLogger(TedScheduler.class);
	private final TedDriver tedDriver;
	private final _TedSchdDriverExt tedSchdDriverExt;
	private final DataSource dataSource;
	private final DaoPostgres dao;
	private ScheduledExecutorService maintenanceExecutor;

	private final Map<String, SchedulerInfo> schedulerTasks = new HashMap<>();


	private static class SchedulerInfo {
		final String name;
		final long taskId;
		final TedRetryScheduler retryScheduler;
		public SchedulerInfo(String name, Long taskId, TedRetryScheduler retryScheduler) {
			this.name = name;
			this.taskId = taskId;
			this.retryScheduler = retryScheduler;
		}
	}

	TedSchedulerImpl(TedDriver tedDriver) {
		this.tedDriver = tedDriver;
		this.tedSchdDriverExt = new _TedSchdDriverExt(tedDriver);
		this.dataSource = tedSchdDriverExt.dataSource();
		this.dao = new DaoPostgres(dataSource, tedSchdDriverExt.systemId());

		maintenanceExecutor = createSchedulerExecutor("TedSchd-");
		maintenanceExecutor.scheduleAtFixedRate(() -> {
			try {
				checkForErrorStatus();
			} catch (Throwable e) {
				logger.error("Error while executing scheduler maintenance tasks", e);
			}
		}, 30, 180, TimeUnit.SECONDS);
	}

	public void shutdown() {
		maintenanceExecutor.shutdown();
	}

	private static int getPostponeSec(TedRetryScheduler retryScheduler) {
		Date startFrom = retryScheduler.getNextRetryTime(null, 1, new Date());
		int postponeSec = 0;
		if (startFrom != null) {
			postponeSec = (int)Math.min(0L, startFrom.getTime() - System.currentTimeMillis() / 1000);
		}
		return postponeSec;
	}

	Long registerScheduler(String taskName, String data, TedProcessorFactory processorFactory, TedRetryScheduler retryScheduler) {
		if (taskName == null || taskName.isEmpty())
			throw new IllegalStateException("task name is required!");
		if (processorFactory == null)
			throw new IllegalStateException("TedProcessorFactory is required!");
		if (retryScheduler == null)
			throw new IllegalStateException("TedRetryScheduler is required!");
		if (! tedSchdDriverExt.isPrimeEnabled())
			throw new IllegalStateException("Prime-instance functionality must be enabled!");
		tedDriver.registerTaskConfig(taskName, processorFactory, retryScheduler);

		// create task is not exists
		int postponeSec = getPostponeSec(retryScheduler);
		Long taskId = createUniqueTask(taskName, data, "", null, postponeSec);
		if (taskId == null)
			throw new IllegalStateException("taskId == null for task " + taskName);

		SchedulerInfo sch = new SchedulerInfo(taskName, taskId, retryScheduler);
		schedulerTasks.put(taskName, sch);
		return taskId;
	}

	void checkForErrorStatus() {
        List<Long> schdTaskIds = schedulerTasks.values().stream().map(sch -> sch.taskId).collect(Collectors.toList());
		List<Long> badTaskIds = dao.checkForErrorStatus(schdTaskIds);
		for (long taskId : badTaskIds) {
			SchedulerInfo schInfo = schedulerTasks.values().stream()
					.filter(sch -> sch.taskId == taskId)
					.findFirst()
					.orElse(null);
			logger.warn("Restore schedule task {} {} from ERROR to RETRY", taskId, schInfo == null ? "null" : schInfo.name);
			TedTask task = tedDriver.getTask(taskId);
			int postponeSec = schInfo == null ? 0 : getPostponeSec(schInfo.retryScheduler);
			dao.restoreFromError(taskId, task.getName(), postponeSec);
		}

	}

	/* creates task only if does not exists (task + activeStatus).
	   While there are not 100% guarantee, but will try to ensure, that 2 processes will not create same task twice (using this method).
	   If task with ERROR status will be found, then it will be converted to RETRY
	*/
	Long createUniqueTask(String name, String data, String key1, String key2, int postponeSec){
		return dao.execWithLockedPrimeTaskId(dataSource, tedSchdDriverExt.primeTaskId(), tx -> {
			List<Long> taskIds = dao.get2ActiveTasks(name, true);
			if (taskIds.size() > 1)
				throw new IllegalStateException("Exists more than one "+ name +" active scheduler task (statuses NEW, RETRY, WORK or ERROR) " + taskIds.toString() + ", skipping");
			if (taskIds.size() == 1) { // exists exactly one task
				Long taskId = taskIds.get(0);
				TedTask tedTask = tedDriver.getTask(taskId);
				if (tedTask.getStatus() == TedStatus.ERROR) {
					logger.info("Restore scheduler task {} {} from error", name, taskId);
					dao.restoreFromError(taskId, name, postponeSec);
					return taskId;
				}
				logger.debug("Exists scheduler task {} with active status (NEW, RETRY or WORK)", name);
				return taskIds.get(0);
			}
			logger.debug("No active scheduler tasks {} exists, will create new", name);
			return tedDriver.createTaskPostponed(name, data, key1, key2, postponeSec);
		});

	}

	static class SchedulerProcessorFactory implements TedProcessorFactory {
		private TedProcessorFactory origTedProcessorFactory;

		public SchedulerProcessorFactory(TedProcessorFactory origTedProcessorFactory) {
			this.origTedProcessorFactory = origTedProcessorFactory;
		}

		@Override
		public TedProcessor getProcessor(String taskName) {
			return new SchedulerProcessor(origTedProcessorFactory.getProcessor(taskName));
		}
	}

	private static class SchedulerProcessor implements TedProcessor {
		private TedProcessor origTedProcessor;

		public SchedulerProcessor(TedProcessor origTedProcessor) {
			this.origTedProcessor = origTedProcessor;
		}

		@Override
		public TedResult process(TedTask task) {
			TedResult result = null;
			try {
				result = origTedProcessor.process(task);
			} catch (Throwable e) {
				logger.warn("Got exception, but will retry anyway: {}", e.getMessage());
				return TedResult.retry(e.getMessage());
			}
			if (result.status == TedStatus.ERROR) {
				logger.warn("Got error, but will retry anyway: {}", result.message);
			}
			return TedResult.retry(result.message);
		}
	}

	static class CronRetry implements TedRetryScheduler {
		private final CronExpression cronExpr;

		public CronRetry(String cron) {
			this.cronExpr = new CronExpression(cron);
		}

		@Override
		public Date getNextRetryTime(TedTask task, int retryNumber, Date startTime) {
			ZonedDateTime ztm = ZonedDateTime.ofInstant(startTime.toInstant(), ZoneId.systemDefault());
			return Date.from(cronExpr.nextTimeAfter(ztm).toInstant());
		}
	}

	static class CustomRetry implements TedRetryScheduler {
		private final TedSchedulerNextTime nextTimeFn;

		public CustomRetry(TedSchedulerNextTime nextTimeFn) {
			this.nextTimeFn = nextTimeFn;
		}

		@Override
		public Date getNextRetryTime(TedTask task, int retryNumber, Date startTime) {
			return nextTimeFn.getNextTime(startTime);
		}
	}

	static class PeriodicRetry implements TedRetryScheduler {
		private final long periodMs;

		public PeriodicRetry(int period, TimeUnit timeUnit) {
			this.periodMs = TimeUnit.MILLISECONDS.convert(period, timeUnit);
		}

		@Override
		public Date getNextRetryTime(TedTask task, int retryNumber, Date startTime) {
			if (startTime == null)
				startTime = new Date();
			return new Date(startTime.getTime() + periodMs);
		}
	}
	//
	//
	//

	private static class SingeInstanceFactory implements TedProcessorFactory {
		private TedProcessor tedProcessor;

		public SingeInstanceFactory(TedProcessor tedProcessor) {
			this.tedProcessor = tedProcessor;
		}

		@Override
		public TedProcessor getProcessor(String taskName) {
			return tedProcessor;
		}
	}

	static class Factory {
		static TedProcessorFactory single(Runnable runnable) {
			return new SchedulerProcessorFactory(new SingeInstanceFactory(task -> {
				runnable.run();
				return TedResult.done();
			}));
		}

		static TedProcessorFactory single(TedProcessor tedProcessor) {
			return new SchedulerProcessorFactory(new SingeInstanceFactory(tedProcessor));
		}

	}

	private ScheduledExecutorService createSchedulerExecutor(final String prefix) {
		ThreadFactory threadFactory = new ThreadFactory() {
			private int counter = 0;
			@Override
			public Thread newThread(Runnable runnable) {
				return new Thread(runnable, prefix + ++counter);
			}
		};
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
		return executor;
	}


}
