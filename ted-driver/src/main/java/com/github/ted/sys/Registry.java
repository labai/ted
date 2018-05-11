package com.github.ted.sys;

import com.github.ted.Ted.TedProcessorFactory;
import com.github.ted.Ted.TedRetryScheduler;
import com.github.ted.sys.ConfigUtils.TedProperty;
import com.github.ted.sys.Model.FieldValidator;
import com.github.ted.sys.RetryConfig.PeriodPatternRetryScheduler;
import com.github.ted.sys.TedDriverImpl.TedContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Augustus
 *         created on 2016.09.16
 *
 * for TED internal usage only!!!
 *
 * tasks and channels configurations
 */
class Registry {
	private final static Logger logger = LoggerFactory.getLogger(Registry.class);
	private final static Logger loggerConfig = LoggerFactory.getLogger("ted-config");

	private final TedContext context;

	private Map<String, TaskConfig> tasks = new ConcurrentHashMap<String, TaskConfig>();
	private Map<String, Channel> channels = new ConcurrentHashMap<String, Channel>();
	private Map<String, Schedule> schedules = new ConcurrentHashMap<String, Schedule>();

	enum TaskType {
		TASK,
		BATCH
	}

	static class Schedule {
		final String name;
		final String produceTask;
		final String cron;

		public Schedule(String name, String produceTask, String cron) {
			this.name = name;
			this.produceTask = produceTask;
			this.cron = cron;
		}
	}

	static class Channel {
		final String name;
		final int workerCount;
		final int taskBufferSize;
		final ThreadPoolExecutor workers;

		public Channel(String name, int workerCount, int taskBufferSize) {
			this.name = name;
			this.workerCount = workerCount;
			this.taskBufferSize = taskBufferSize;
			// create queue bigger by workerCount - to be sure queue will not be oversize (on very fast tasks RejectedExecutionException occurs).
			// this count will be deducted when calculate pack size
			this.workers = TedDriverImpl.createWorkersExecutor(name, workerCount, taskBufferSize + workerCount);
		}

	}

	static class TaskConfig {
		final String taskName;
		final TedProcessorFactory tedProcessorFactory;
		final int workTimeoutMinutes;
		final String channel;
		final TedRetryScheduler retryScheduler;
		final TaskType taskType;
		final String batchTask;
		final int batchTimeoutMinutes;
		final String shortLogName; // 5 letters name for threadName

		public TaskConfig(String taskName, TedProcessorFactory tedProcessorFactory,
				int workTimeoutMinutes, TedRetryScheduler retryScheduler, String channel,
				TaskType taskType, String batchTask, int batchTimeoutMinutes) {
			this.taskName = taskName;
			this.tedProcessorFactory = tedProcessorFactory;
			this.workTimeoutMinutes = Math.max(workTimeoutMinutes, 1); // timeout, less than 1 minute, is invalid, as process will check timeouts only >= 1 min
			this.retryScheduler = retryScheduler;
			this.channel = channel == null ? Model.CHANNEL_MAIN : channel;
			this.taskType = taskType == null ? TaskType.TASK : taskType;
			this.batchTask = batchTask;
			this.batchTimeoutMinutes = batchTimeoutMinutes;
			this.shortLogName = makeShortName(taskName);
		}

	}

	// not public (for internal Ted class only!)
	public interface ITedProcessorFactory {
	}

	static String makeShortName(String taskName) {
		final int prefixLen = 2, hashLen = 3;
		if (taskName.length() <= prefixLen + hashLen)
			return taskName.toUpperCase();
		String prefix = (taskName.replace("-", "").replace("_", "")+ "XX").substring(0, prefixLen);
		String hash = "XXX" + Integer.toString(Math.abs(taskName.hashCode()), 36);
		hash = hash.substring(hash.length() - hashLen);
		return (prefix + hash).toUpperCase();
	}


	public Registry(TedContext context) {
		this.context = context;
	}

	//
	// tasks
	//
	/** register task with default/ted.properties settings */
	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory) {
		registerTaskConfig(taskName, tedProcessorFactory, null, null, null);
	}
	/* register task with default/ted.properties settings, but overwrote with properties param */
//	private void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, Properties properties) {
//		//Map<String, Properties> shortPropMap = ConfigUtils.getShortPropertiesByPrefix(properties, ConfigUtils.PROPERTY_PREFIX_TASK);
//		Properties shortProp = new Properties();
//
//		Properties tmp = context.config.taskMap().get(taskName);
//		if (tmp != null)
//			shortProp.putAll(tmp);
//
//		//tmp = shortPropMap.get(taskName);
//		//if (tmp != null)
//		//	shortProp.putAll(tmp); // will overwrite
//
//		Integer workTimeoutInMinutes = ConfigUtils.getInteger(shortProp, ConfigUtils.TedProperty.TASK_TIMEOUT_MINUTES, null);
//		String retryPattern = ConfigUtils.getString(shortProp, ConfigUtils.TedProperty.TASK_RETRY_PATTERN, null);
//		String channel = ConfigUtils.getString(shortProp, ConfigUtils.TedProperty.TASK_CHANNEL, null);
//
//		registerTaskConfig(taskName, tedProcessorFactory, workTimeoutInMinutes, retryPattern, channel);
//	}

	void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory,
			Integer workTimeoutInMinutes, TedRetryScheduler retryScheduler, String channel) {

		if (tasks.containsKey(taskName)) {
			logger.warn("Task '" + taskName + "' already exists in registry, skip to register new one");
			return;
		}

		// overwrite parameters from config
		Properties shortProp = context.config.taskMap().get(taskName);
		workTimeoutInMinutes = ConfigUtils.getInteger(shortProp, ConfigUtils.TedProperty.TASK_TIMEOUT_MINUTES, workTimeoutInMinutes);
		channel = ConfigUtils.getString(shortProp, ConfigUtils.TedProperty.TASK_CHANNEL, channel);

		// assign defaults if nulls
		if (workTimeoutInMinutes == null)
			workTimeoutInMinutes = context.config.defaultTaskTimeoutMn();
		//if (retryPattern == null)
		//	retryPattern = context.config.defaultRetryPauses();

		int batchTimeoutInMinutes = ConfigUtils.getInteger(shortProp, ConfigUtils.TedProperty.TASK_BATCH_TIMEOUT_MINUTES, context.config.defaultBatchTaskTimeoutMn());

		if (channel == null)
			channel = Model.CHANNEL_MAIN;
		if (!Model.CHANNEL_MAIN.equals(channel)) {
			if (getChannel(channel) == null)
				throw new IllegalArgumentException("Channel '" + channel + "' does not exists");
		}

		if (retryScheduler == null) {
			String retryPattern = ConfigUtils.getString(shortProp, TedProperty.TASK_RETRY_PAUSES, context.config.defaultRetryPauses());
			retryScheduler = new PeriodPatternRetryScheduler(retryPattern);
		}

		String taskTypeStr = ConfigUtils.getString(shortProp, TedProperty.TASK_TYPE, TaskType.TASK.toString());
		TaskType taskType;
		try {
			taskType = TaskType.valueOf(taskTypeStr);
		} catch (IllegalArgumentException e) {
			logger.warn("Invalid taskType value ({}) for task {}, allowed {}", taskTypeStr, taskName, Arrays.asList(TaskType.values()));
			taskType = TaskType.TASK;
		}

		String batchTask = ConfigUtils.getString(shortProp, TedProperty.TASK_BATCH_TASK, null);

//		if (taskType == TaskType.BATCH) {
//			logger.debug("Setting batchInterceptProcessorFactory for task {}", taskName);
//			tedProcessorFactory = context.batchManager.batchInterceptProcessorFactory(tedProcessorFactory);
//		}

		TaskConfig ttc = new TaskConfig(taskName, tedProcessorFactory, workTimeoutInMinutes, retryScheduler, channel, taskType, batchTask, batchTimeoutInMinutes);
		tasks.put(taskName, ttc);
		loggerConfig.info("Register task {} (channel={} timeoutMinutes={} logid={} type={}{}{})", ttc.taskName, ttc.channel, ttc.workTimeoutMinutes, ttc.shortLogName, taskType, (batchTask==null?"":" batchTask="+batchTask), (taskType==TaskType.BATCH?" batchTimeoutInMinutes="+batchTimeoutInMinutes:""));
	}

	public TaskConfig getTaskConfig(String taskName) {
		return tasks.get(taskName);
	}

	//
	// channels
	// (create internally in TedDriverImpl)
	//
	void registerChannel(String channel, Properties shortProperties) {
		int workerCount = ConfigUtils.getInteger(shortProperties, ConfigUtils.TedProperty.CHANNEL_WORKERS_COUNT, 5);
		int bufferSize = ConfigUtils.getInteger(shortProperties, ConfigUtils.TedProperty.CHANNEL_TASK_BUFFER, 200);
		registerChannel(channel, workerCount, bufferSize);
	}

	void registerChannel(String channel, int workerCount, int bufferSize) {
		if (workerCount < 1 || workerCount > 1000)
			throw new IllegalArgumentException("Worker count must be number between 1 and 1000, channel=" + channel);
		FieldValidator.validateTaskChannel(channel);
		if (tasks.containsKey(channel)) {
			logger.warn("Channel '" + channel + "' already exists in registry, skip to register new one");
			return;
		}
		Channel ochan = new Channel(channel, workerCount, bufferSize);
		channels.put(channel, ochan);
		loggerConfig.info("Register channel {} (workerCount={} taskBufferSize={})", ochan.name, ochan.workerCount, ochan.taskBufferSize);
	}

	public Channel getChannel(String name) {
		return channels.get(name);
	}

	Collection<Channel> getChannels() {
		return Collections.unmodifiableCollection(channels.values());
	}

	//
	// schedules
	// (create internally in TedDriverImpl)
	//
/*
	void registerSchedule(String scheduleName, Properties shortProperties) {
		String produceTask = ConfigUtils.getString(shortProperties, TedProperty.SCHEDULE_PRODUCE_TASK, null);
		String cron = ConfigUtils.getString(shortProperties, TedProperty.SCHEDULE_CRON, null);
		registerSchedule(scheduleName, produceTask, cron);
	}


	void registerSchedule(String scheduleName, String produceTask, String cron) {
		FieldValidator.validateTaskChannel(scheduleName);
		if (tasks.containsKey(scheduleName)) {
			logger.warn("Schedule '" + scheduleName + "' already exists in registry, skip to register new one");
			return;
		}
		if (FieldValidator.isEmpty(produceTask))
			throw new IllegalArgumentException("Parameter 'produceTask' is required for schedule '" + scheduleName + "'");
		if (FieldValidator.isEmpty(cron))
			throw new IllegalArgumentException("Parameter 'cron' is required for schedule '" + scheduleName + "'");

		logger.info("Register schedule "+ scheduleName +" with produceTask="+ produceTask +", cron="+ cron);
		Schedule schedule = new Schedule(scheduleName, produceTask, cron);
		schedules.put(scheduleName, schedule);
	}

	Collection<Schedule> getSchedules() {
		return Collections.unmodifiableCollection(schedules.values());
	}
*/

}
