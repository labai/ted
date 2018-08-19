package labai.ted.scheduler;

import labai.ted.Ted.TedProcessor;
import labai.ted.Ted.TedProcessorFactory;
import labai.ted.Ted.TedRetryScheduler;
import labai.ted.TedDriver;
import labai.ted.scheduler.TedSchedulerImpl.CronRetry;
import labai.ted.scheduler.TedSchedulerImpl.CustomRetry;
import labai.ted.scheduler.TedSchedulerImpl.Factory;
import labai.ted.scheduler.TedSchedulerImpl.PeriodicRetry;
import labai.ted.scheduler.TedSchedulerImpl.SchedulerProcessorFactory;
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

	//
	// cron
	//
	public void registerCronScheduler(String name, String data, TedProcessorFactory processorFactory, String cron) {
		tedSchedulerImpl.registerScheduler(name, data, new SchedulerProcessorFactory(processorFactory), new CronRetry(cron));
	}

	// will call same runnable each time
//	public void registerCronScheduler(String name, Runnable runnable, String cron) {
//		tedSchedulerImpl.registerScheduler(name, Factory.single(runnable), new CronRetry(cron));
//	}
//
//	public void registerCronScheduler(String name, TedProcessor tedProcessor, String cron) {
//		tedSchedulerImpl.registerScheduler(name, Factory.single(tedProcessor), new CronRetry(cron));
//	}

	//
	// custom
	//
	public void registerCustomScheduler(String name, String data, TedProcessorFactory processorFactory, TedSchedulerNextTime nextTimeFn) {
		tedSchedulerImpl.registerScheduler(name, data, new SchedulerProcessorFactory(processorFactory), new CustomRetry(nextTimeFn));
	}

//	public void registerCustomScheduler(String name, Runnable runnable, TedSchedulerNextTime nextTimeFn) {
//		tedSchedulerImpl.registerScheduler(name, Factory.single(runnable), new CustomRetry(nextTimeFn));
//	}
//
//	public void registerCustomScheduler(String name, TedProcessor tedProcessor, TedSchedulerNextTime nextTimeFn) {
//		tedSchedulerImpl.registerScheduler(name, Factory.single(tedProcessor), new CustomRetry(nextTimeFn));
//	}

	//
	// periodic
	//
	public void registerPeriodicScheduler(String name, String data, TedProcessorFactory processorFactory, int period, TimeUnit timeUnit) {
		tedSchedulerImpl.registerScheduler(name, data, new SchedulerProcessorFactory(processorFactory), new PeriodicRetry(period, timeUnit));
	}

//	public void registerPeriodicScheduler(String name, Runnable runnable, int period, TimeUnit timeUnit) {
//		tedSchedulerImpl.registerScheduler(name, Factory.single(runnable), new PeriodicRetry(period, timeUnit));
//	}
//
//	public void registerPeriodicScheduler(String name, TedProcessor tedProcessor, int period, TimeUnit timeUnit) {
//		tedSchedulerImpl.registerScheduler(name, Factory.single(tedProcessor), new PeriodicRetry(period, timeUnit));
//	}

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

		public void register(){
			tedSchedulerImpl.registerScheduler(taskName, data, tedProcessorFactory, tedRetryScheduler);
		}
	}

	public SchedulerBuilder builder() {
		return new SchedulerBuilder();
	}

}
