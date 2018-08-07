package com.github.labai.ted.sys;


import com.github.labai.ted.Ted.TedDbType;
import com.github.labai.ted.Ted.TedPackProcessorFactory;
import com.github.labai.ted.Ted.TedProcessorFactory;
import com.github.labai.ted.Ted.TedRetryScheduler;
import com.github.labai.ted.Ted.TedStatus;
import com.github.labai.ted.Ted.TedTask;
import com.github.labai.ted.sys.ConfigUtils.TedConfig;
import com.github.labai.ted.sys.ConfigUtils.TedProperty;
import com.github.labai.ted.sys.Model.FieldValidator;
import com.github.labai.ted.sys.Model.TaskParam;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.Registry.Channel;
import com.github.labai.ted.sys.Registry.TaskConfig;
import com.github.labai.ted.sys.TaskManager.TedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 * for TED internal usage only!!!
 *
 * public class for TedDriver usage only
 *
 */
public class TedDriverImpl {
	private final static Logger logger = LoggerFactory.getLogger(TedDriverImpl.class);

	private static int driverLocalInstanceCounter = 0; // expected always 1, more for testings
	private final int localInstanceNo = ++TedDriverImpl.driverLocalInstanceCounter;
	final String tedNamePrefix = localInstanceNo == 1 ? "Ted" : "Te" + localInstanceNo;

	/* contains loaded libs, services, dao and other singletons */
	static class TedContext {
		TedDriverImpl tedDriver;
		TedConfig config;
		Registry registry;
		TedDao tedDao;
		TaskManager taskManager;
		RetryConfig retryConfig;
		QuickCheck quickCheck;
		PrimeInstance prime;
		EventQueueManager eventQueueManager;
		//BatchManager batchManager;
		//ScheduleManager scheduleManager;
	}

	private final TedContext context;
	private AtomicBoolean isStartedFlag = new AtomicBoolean(false);
	private ScheduledExecutorService driverExecutor;
	private ScheduledExecutorService maintenanceExecutor;

	public TedDriverImpl(TedDbType dbType, DataSource dataSource, String system) {
		this(dbType, dataSource, system, null);
	}

	public TedDriverImpl(TedDbType dbType, DataSource dataSource, Properties properties) {
		this(dbType, dataSource, null, properties);
	}

	TedDriverImpl(TedDbType dbType, DataSource dataSource, String system, Properties properties) {
		if (properties != null && properties.containsKey(TedProperty.SYSTEM_ID))
			system = properties.getProperty(TedProperty.SYSTEM_ID);
		FieldValidator.validateTaskSystem(system);
		this.context = new TedContext();
		context.tedDriver = this;
		context.config = new TedConfig(system);
		context.tedDao = dbType == TedDbType.ORACLE
				? new TedDaoOracle(system, dataSource)
				: new TedDaoPostgres(system, dataSource);
		context.registry = new Registry(context);
		context.taskManager = new TaskManager(context);
		context.retryConfig = new RetryConfig(context);
		//context.batchManager = new BatchManager(context);
		//context.scheduleManager = new ScheduleManager(context);
		context.quickCheck = new QuickCheck(context);
		context.prime = new PrimeInstance(context);
		context.eventQueueManager = new EventQueueManager(context);


		// read properties (e.g. from ted.properties.
		// default MAIN channel configuration: 5/100. Can be overwrite by [properties]
		// default QUEUE channel configuration: 2/100
		//
		// e.g.: ted.channel.MAIN.workersMin
		Properties defaultChanProp = new Properties();
		String prefixMain = ConfigUtils.PROPERTY_PREFIX_CHANNEL + Model.CHANNEL_MAIN + ".";
		defaultChanProp.put(prefixMain + TedProperty.CHANNEL_WORKERS_COUNT, "5");
		defaultChanProp.put(prefixMain + TedProperty.CHANNEL_TASK_BUFFER, "100");
		String prefixQueue = ConfigUtils.PROPERTY_PREFIX_CHANNEL + Model.CHANNEL_QUEUE + ".";
		defaultChanProp.put(prefixQueue + TedProperty.CHANNEL_WORKERS_COUNT, "2");
		defaultChanProp.put(prefixQueue + TedProperty.CHANNEL_TASK_BUFFER, "100");
		ConfigUtils.readTedProperties(context.config, defaultChanProp);
		ConfigUtils.readTedProperties(context.config, properties);

		// Create channels
		for (String channel : context.config.channelMap().keySet()) {
			context.registry.registerChannel(channel, context.config.channelMap().get(channel));
		}

//		// register schedules
//		for (String schedule : context.config.scheduleMap().keySet()) {
//			context.registry.registerSchedule(schedule, context.config.scheduleMap().get(schedule));
//		}
//		context.tedDao.createUpdateSchedules(context.registry.getSchedules());

	}

	public void start() {
		if (!isStartedFlag.compareAndSet(false, true)) {
			logger.warn("TED driver is already started!");
			return;
		}
		logger.info("Starting TED driver");
		ConfigUtils.printConfigToLog(context.config);

		// init
		context.prime.init();

		// driver
		driverExecutor = createSchedulerExecutor(tedNamePrefix + "Driver-");;
		driverExecutor.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					context.quickCheck.quickCheck();
				} catch (Exception e) {
					logger.error("Error while executing driver task", e);
				}
			}
		}, context.config.initDelayMs(), context.config.intervalDriverMs(), TimeUnit.MILLISECONDS);

		// maintenance tasks processor
		maintenanceExecutor = createSchedulerExecutor(tedNamePrefix + "Maint-");
		maintenanceExecutor.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					context.taskManager.processMaintenanceTasks();
				} catch (Exception e) {
					logger.error("Error while executing maintenance tasks", e);
				}
			}
		}, context.config.initDelayMs(), context.config.intervalMaintenanceMs(), TimeUnit.MILLISECONDS);
	}

	public void shutdown(long timeoutMs) {
		long startMs = System.currentTimeMillis();
		long tillMs = startMs + (timeoutMs > 0 ? timeoutMs : 20 * 1000);
		if (isStartedFlag.get() == false) {
			logger.info("TED driver is not started, leaving shutdown procedure");
			return;
		}
		logger.debug("Start to shutdown TED driver");

		// shutdown (stop accept new tasks)
		driverExecutor.shutdown();
		maintenanceExecutor.shutdown();
		for (Channel channel : context.registry.getChannels()) {
			channel.workers.shutdown();
		}
		List<TedRunnable> tasksToReturn = new ArrayList<TedRunnable>();
		for (Channel channel : context.registry.getChannels()) {
			List<Runnable> chanTasks = channel.workers.shutdownNow();
			for (Runnable r : chanTasks) {
				tasksToReturn.add((TedRunnable) r);
			}
		}
		// return back not started tasks to status NEW
		for (TedRunnable tedr : tasksToReturn) {
			for (TaskRec task : tedr.getTasks()) {
				logger.info("return back task {} (taskId={}) to status NEW", task.name, task.taskId);
				context.tedDao.setStatusPostponed(task.taskId, TedStatus.NEW, "return on shutdown", new Date());
			}
		}

		// wait for finish
		logger.debug("waiting for finish TED tasks...");
		Map<String, ExecutorService> pools = new LinkedHashMap<String, ExecutorService>();
		pools.put("(driver)", driverExecutor);
		pools.put("(maintenance)", maintenanceExecutor);
		for (Channel channel : context.registry.getChannels()) {
			pools.put(channel.name, channel.workers);
		}
		boolean interrupt = false;
		for (String poolId : pools.keySet()) {
			ExecutorService pool = pools.get(poolId);
			try {
				if (!pool.awaitTermination(Math.max(tillMs - System.currentTimeMillis(), 0L), TimeUnit.MILLISECONDS)) {
					logger.warn("WorkerPool {} did not terminated successfully", poolId);
				}
			} catch (InterruptedException e) {
				interrupt = true;
			}
		}
		if (interrupt)
			Thread.currentThread().interrupt(); // Preserve interrupt status (??? see ThreadPoolExecutor javadoc)
		isStartedFlag.set(false);
		logger.info("TED driver shutdown in {}ms", System.currentTimeMillis() - startMs);
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

	static ThreadPoolExecutor createWorkersExecutor(final String threadPrefix, int workerCount, int queueSize) {
		ThreadFactory threadFactory = new ThreadFactory() {
			private int counter = 0;
			@Override
			public Thread newThread(Runnable runnable) {
				return new Thread(runnable, threadPrefix + "-" + ++counter);
			}
		};
		ThreadPoolExecutor executor = new ThreadPoolExecutor(workerCount, workerCount,
				0, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(queueSize), threadFactory);
		return executor;
	}

	/*
	 * "public" for TedDriver only
	 */

	public Long createTask(String taskName, String data, String key1, String key2, Long batchId) {
		FieldValidator.validateTaskData(data);
		FieldValidator.validateTaskKey1(key1);
		FieldValidator.validateTaskKey2(key2);
		TaskConfig tc = context.registry.getTaskConfig(taskName);
		if (tc == null)
			throw new IllegalArgumentException("Task '" + taskName + "' is not known for TED");
		return context.tedDao.createTask(taskName, tc.channel, data, key1, key2, batchId);
	}

/*
	public Long createTaskUniqueKey1(String taskName, String data, String key1, String key2) {
		FieldValidator.validateTaskData(data);
		FieldValidator.validateTaskKey1(key1);
		FieldValidator.validateTaskKey2(key2);
		if (key1 == null || key1.isEmpty())
			throw new FieldValidateException("key1 must be not empty");
		TaskConfig tc = context.registry.getTaskConfig(taskName);
		if (tc == null)
			throw new IllegalArgumentException("Task '" + taskName + "' is not known for TED");
		if (context.tedDao.existsActiveTaskByKey1(taskName, key1)) {
			logger.debug("duplicate was found for unique (on check) task={}, key1={}, skip task creation", taskName, key1);
			return null;
		}
		try {
			return context.tedDao.createTask(taskName, tc.channel, data, key1, key2, null);
		} catch (TedSqlException e) {
			if (e.getCause() != null && e.getCause() instanceof SQLException) {
				SQLException sqle = (SQLException) e.getCause();
				if ("23505".equals(sqle.getSQLState())) { // 23505 in postgres: duplicate key value violates unique constraint
					logger.info("duplicate was found for unique (unique index) task={}, key1={}, skip task creation", taskName, key1);
					return null;
				}
			}
			throw e;
		}
	}
*/

	Long createTask(String taskName, String data, String key1, String key2) {
		return createTask(taskName, data, key1, key2, null);
	}

	public Long createTaskPostponed(String taskName, String data, String key1, String key2, int postponeSec) {
		FieldValidator.validateTaskData(data);
		FieldValidator.validateTaskKey1(key1);
		FieldValidator.validateTaskKey2(key2);
		TaskConfig tc = context.registry.getTaskConfig(taskName);
		if (tc == null)
			throw new IllegalArgumentException("Task '" + taskName + "' is not known for TED");
		return context.tedDao.createTaskPostponed(taskName, tc.channel, data, key1, key2, postponeSec);
	}

	// create task and execute it
	// (or add task to execution queue - channel)
	public Long createAndExecuteTask(String taskName, String data, String key1, String key2, boolean inChannel) {
		FieldValidator.validateTaskData(data);
		FieldValidator.validateTaskKey1(key1);
		FieldValidator.validateTaskKey2(key2);
		TaskConfig tc = context.registry.getTaskConfig(taskName);
		if (tc == null)
			throw new IllegalArgumentException("Task '" + taskName + "' is not known for TED");
		Long taskId = context.tedDao.createTaskWithWorkStatus(taskName, tc.channel, data, key1, key2);
		TaskRec taskRec = new TaskRec();
		taskRec.taskId = taskId;
		taskRec.batchId = null;
		taskRec.system = context.config.systemId();
		taskRec.name = taskName;
		taskRec.status = TedStatus.WORK.toString();
		taskRec.channel = tc.channel;
		taskRec.nextTs = new Date();
		taskRec.retries = 0;
		taskRec.key1 = key1;
		taskRec.key2 = key2;
		taskRec.data = data;
		taskRec.createTs = new Date();
		taskRec.startTs = new Date();
		taskRec.finishTs = null;
		if (inChannel) {
			// in separate thread
			context.taskManager.sendTaskListToChannels(Collections.singletonList(taskRec));
		} else {
			// in this thread
			context.taskManager.processTask(Collections.singletonList(taskRec));
		}
		return taskId;
	}

	public List<Long> createTasksBulk(List<TedTask> tedTasks, Long batchId) {
		//if (context.tedDao.getDbType() != DbType.POSTGRES)
		//	throw new IllegalArgumentException("createTasksBulk allowed only for PostgreSql db");
		if (tedTasks == null || tedTasks.isEmpty())
			return Collections.emptyList();

		List<TaskParam> taskParams = new ArrayList<TaskParam>();
		for (TedTask task : tedTasks) {
			if (task.getTaskId() != null)
				throw new IllegalArgumentException("taskId must be null for parameter (task=" + task.getName() + " taskId=" + task.getTaskId() + "");
			TaskConfig tc = context.registry.getTaskConfig(task.getName());
			if (tc == null)
				throw new IllegalArgumentException("Task '" + task.getName() + "' is not known for TED");
			TaskParam tp = new TaskParam();
			tp.taskId = task.getTaskId();
			tp.name = task.getName();
			tp.key1 = task.getKey1();
			tp.key2 = task.getKey2();
			tp.data = task.getData();
			tp.channel = tc.channel;
			tp.batchId = batchId; //task.getBatchId();
			taskParams.add(tp);
		}
		return context.tedDao.createTasksBulk(taskParams);
	}

	// create tasks by list and batch task for them. return batch taskId
//	public Long createBatch(List<TedTask> tedTasks) {
//		return createBatch(null, null, null, null, tedTasks);
//	}

	// create tasks by list and batch task for them. return batch taskId
	// if batchTaskName is null - will take from taskConfiguration
	public Long createBatch(String batchTaskName, String data, String key1, String key2, List<TedTask> tedTasks) {
		if (tedTasks == null || tedTasks.isEmpty())
			return null;
		if (batchTaskName == null) {
			throw new IllegalStateException("batchTaskName is required (yet, TODO?)!");
			/*
			TedTask task = tedTasks.get(0);
			TaskConfig taskConfig = context.registry.getTaskConfig(task.getName());
			if (taskConfig.batchTask == null)
				throw new IllegalArgumentException("Batch task is not configured for task '" + taskConfig.taskName + "'");
			batchTaskName = taskConfig.batchTask;
			*/
		}
		TaskConfig batchTC = context.registry.getTaskConfig(batchTaskName);
		if (batchTC == null)
			throw new IllegalArgumentException("Batch task '" + batchTaskName + "' is not known for TED");

		Long batchId = context.tedDao.createTaskPostponed(batchTC.taskName, batchTC.channel, data, key1, key2, 30 * 60);
		createTasksBulk(tedTasks, batchId);
		context.tedDao.setStatusPostponed(batchId, TedStatus.NEW, Model.BATCH_MSG, new Date());

		return batchId;
	}

	public Long createEvent(String taskName, String discriminator, String data, String key2) {
		return context.eventQueueManager.createEvent(taskName, discriminator, data, key2);
	}

	public Long createAndTryExecuteEvent(String taskName, String discriminator, String data, String key2) {
		return context.eventQueueManager.createAndTryExecuteEvent(taskName, discriminator, data, key2);
	}

	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory) {
		FieldValidator.validateTaskName(taskName);
		context.registry.registerTaskConfig(taskName, tedProcessorFactory);
	}

	public void registerTaskConfig(String taskName, TedPackProcessorFactory tedPackProcessorFactory) {
		FieldValidator.validateTaskName(taskName);
		context.registry.registerTaskConfig(taskName, tedPackProcessorFactory);
	}

	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, Integer workTimeoutInMinutes, TedRetryScheduler retryScheduler, String channel) {
		FieldValidator.validateTaskName(taskName);
		context.registry.registerTaskConfig(taskName, tedProcessorFactory, null, workTimeoutInMinutes, retryScheduler, channel);
	}

	public void registerChannel(String channel, int workerCount, int taskBufferSize) {
		context.registry.registerChannel(channel, workerCount, taskBufferSize);
	}

	public PrimeInstance prime() {
		return context.prime;
	}
	//
	// package scoped - for tests only
	//
	TedContext getContext() {
		return context;
	}
}
