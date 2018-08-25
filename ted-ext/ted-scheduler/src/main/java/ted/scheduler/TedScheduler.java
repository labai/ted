package ted.scheduler;

import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedRetryScheduler;
import ted.driver.TedDriver;
import ted.scheduler.TedSchedulerImpl.CronRetry;
import ted.scheduler.TedSchedulerImpl.CustomRetry;
import ted.scheduler.TedSchedulerImpl.Factory;
import ted.scheduler.TedSchedulerImpl.PeriodicRetry;
import ted.scheduler.TedSchedulerImpl.SchedulerProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author Augustus
 *         created on 2018.08.16
 *
 *  TedScheduler with api
 *
 */
public class TedScheduler {
	private static final Logger logger = LoggerFactory.getLogger(TedScheduler.class);
	private TedSchedulerImpl tedSchedulerImpl;

	public interface TedSchedulerNextTime {
		Date getNextTime(Date startTime);
	}

	public TedScheduler(TedDriver tedDriver) {
		this.tedSchedulerImpl = new TedSchedulerImpl(tedDriver);
	}

	public void shutdown() {
		tedSchedulerImpl.shutdown();
	}

	//
	// cron
	//
	public void registerCronScheduler(String name, String data, TedProcessorFactory processorFactory, String cron) {
		tedSchedulerImpl.registerScheduler(name, data, new SchedulerProcessorFactory(processorFactory), new CronRetry(cron));
	}

	//
	// custom
	//
	public void registerCustomScheduler(String name, String data, TedProcessorFactory processorFactory, TedSchedulerNextTime nextTimeFn) {
		tedSchedulerImpl.registerScheduler(name, data, new SchedulerProcessorFactory(processorFactory), new CustomRetry(nextTimeFn));
	}

	//
	// periodic
	//
	public void registerPeriodicScheduler(String name, String data, TedProcessorFactory processorFactory, int period, TimeUnit timeUnit) {
		tedSchedulerImpl.registerScheduler(name, data, new SchedulerProcessorFactory(processorFactory), new PeriodicRetry(period, timeUnit));
	}

	public class SchedulerBuilder {
		String taskName;
		TedRetryScheduler tedRetryScheduler;
		TedProcessorFactory tedProcessorFactory;
		String data = null;

		public SchedulerBuilder name(String taskName) {
			this.taskName = taskName;
			return this;
		}
		public SchedulerBuilder processor(TedProcessor tedProcessor) {
			this.tedProcessorFactory = Factory.single(tedProcessor);
			return this;
		}
		public SchedulerBuilder runnable(Runnable runnable) {
			this.tedProcessorFactory = Factory.single(runnable);
			return this;
		}
		public SchedulerBuilder processorFactory(TedProcessorFactory tedProcessorFactory) {
			this.tedProcessorFactory = tedProcessorFactory;
			return this;
		}

		public SchedulerBuilder scheduleCustom(TedSchedulerNextTime nextTimeFn) {
			this.tedRetryScheduler = new CustomRetry(nextTimeFn);
			return this;
		}
		public SchedulerBuilder schedulePeriodic(int period, TimeUnit timeUnit) {
			this.tedRetryScheduler = new PeriodicRetry(period, timeUnit);
			return this;
		}
		public SchedulerBuilder scheduleCron(String cron) {
			this.tedRetryScheduler = new CronRetry(cron);
			return this;
		}
		public SchedulerBuilder data(String data) {
			this.data = data;
			return this;
		}

		public Long register(){
			return tedSchedulerImpl.registerScheduler(taskName, data, tedProcessorFactory, tedRetryScheduler);
		}
	}

	public SchedulerBuilder builder() {
		return new SchedulerBuilder();
	}

}
