package labai.ted.scheduler;

import labai.ted.Ted.TedProcessor;
import labai.ted.Ted.TedProcessorFactory;
import labai.ted.Ted.TedRetryScheduler;
import labai.ted.Ted.TedStatus;
import labai.ted.TedDriver;
import labai.ted.TedResult;
import labai.ted.TedTask;
import labai.ted.scheduler.TedScheduler.TedSchedulerNextTime;
import labai.ted.scheduler.utils.CronExpression;
import labai.ted.sys.TedSchdDriverExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author Augustus
 *         created on 2018.08.16
 *
 *  for TED internal usage only!!!
 *
 *
 *
 */
class TedSchedulerImpl {
	private static final Logger logger = LoggerFactory.getLogger(TedScheduler.class);
	private final TedDriver tedDriver;
	private final TedSchdDriverExt tedSchdDriverExt;
	private final DataSource dataSource;
	private final DaoPostgres dao;

	TedSchedulerImpl(TedDriver tedDriver) {
		this.tedDriver = tedDriver;
		this.tedSchdDriverExt = new TedSchdDriverExt(tedDriver);
		this.dataSource = tedSchdDriverExt.dataSource();
		this.dao = new DaoPostgres(dataSource, tedSchdDriverExt.systemId());
	}

	void registerScheduler(String taskName, String data, TedProcessorFactory processorFactory, TedRetryScheduler retryScheduler) {
		if (! tedSchdDriverExt.isPrimeEnabled())
			throw new IllegalStateException("Prime-instance functionality must be enabled!");
		tedDriver.registerTaskConfig(taskName, processorFactory, retryScheduler);

		// create task is not exists
		Date startFrom = retryScheduler.getNextRetryTime(null, 1, new Date());
		int postponeSec = 0;
		if (startFrom != null) {
			postponeSec = (int)Math.min(0L, startFrom.getTime() - System.currentTimeMillis() / 1000);
		}
		createUniqueTask(taskName, data, "", null, postponeSec);
	}


	/* creates task only if does not exists (task + activeStatus).
	   While there are not 100% guarantee, but will try to ensure, that 2 processes will not create same task twice (using this method).
	   Key1 can be "" if task must be unique.
	*/
	Long createUniqueTask(String name, String data, String key1, String key2, int postponeSec){
		return dao.execWithLockedPrimeTaskId(dataSource, tedSchdDriverExt.primeTaskId(), tx -> {
			if (dao.existsActiveTask(name)) {
				logger.debug("Exists task {} with key1='{}' and active status (NEW, RETRY or WORK), skipping", name, key1);
				return null;
			}

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

}
