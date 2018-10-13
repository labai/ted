package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedRetryScheduler;
import ted.driver.Ted.TedStatus;
import ted.driver.TedTask;
import ted.driver.sys.Trash.TedMetricsEvents;
import ted.driver.sys.ConfigUtils.TedConfig;
import ted.driver.sys.ConfigUtils.TedProperty;
import ted.driver.sys.Executors.ChannelThreadPoolExecutor;
import ted.driver.sys.Executors.TedRunnable;
import ted.driver.sys.Model.FieldValidator;
import ted.driver.sys.Model.TaskParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.Registry.TaskConfig;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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
public final class TedDriverImpl {
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
		TedDaoExt tedDaoExt;
		TaskManager taskManager;
		Executors executors;
		RetryConfig retryConfig;
		QuickCheck quickCheck;
		PrimeInstance prime;
		EventQueueManager eventQueueManager;
		BatchWaitManager batchWaitManager;
		NotificationManager notificationManager;
		Stats stats;
	}

	final DataSource dataSource;
	private final TedContext context;
	private AtomicBoolean isStartedFlag = new AtomicBoolean(false);
	private ScheduledExecutorService driverExecutor;
	private ScheduledExecutorService maintenanceExecutor;
	private final ExecutorService statsEventExecutor;

	public TedDriverImpl(TedDbType dbType, DataSource dataSource, String system) {
		this(dbType, dataSource, system, null);
	}

	public TedDriverImpl(TedDbType dbType, DataSource dataSource, Properties properties) {
		this(dbType, dataSource, null, properties);
	}

	TedDriverImpl(TedDbType dbType, DataSource dataSource, String system, Properties properties) {
		this.dataSource = dataSource;
		if (properties != null && properties.containsKey(TedProperty.SYSTEM_ID))
			system = properties.getProperty(TedProperty.SYSTEM_ID);
		FieldValidator.validateTaskSystem(system);
		this.context = new TedContext();
		context.tedDriver = this;
		context.config = new TedConfig(system);
		context.executors = new Executors(context);

		this.statsEventExecutor = context.executors.createExecutor(
				tedNamePrefix + "Stats" , 1, 2000);
		context.stats = new Stats(statsEventExecutor);

		switch (dbType) {
			case POSTGRES:
				TedDaoPostgres pg = new TedDaoPostgres(system, dataSource, context.stats);
				context.tedDao = pg;
				context.tedDaoExt = pg;
				break;
			case ORACLE:
				context.tedDao = new TedDaoOracle(system, dataSource, context.stats);
				context.tedDaoExt = new TedDaoExtNA("Oracle");
				break;
			case MYSQL:
				context.tedDao = new TedDaoMysql(system, dataSource, context.stats);
				context.tedDaoExt = new TedDaoExtNA("MySql");
				break;
			default: throw new IllegalStateException("Invalid case " + dbType);
		}
		context.registry = new Registry(context);
		context.taskManager = new TaskManager(context);
		context.retryConfig = new RetryConfig(context);
		context.quickCheck = new QuickCheck(context);
		context.prime = new PrimeInstance(context);
		context.eventQueueManager = new EventQueueManager(context);
		context.batchWaitManager = new BatchWaitManager(context);
		context.notificationManager = new NotificationManager(context);

		// read properties (e.g. from ted.properties.
		// default MAIN channel configuration: 5/100. Can be overwrite by [properties]
		//
		Properties defaultChanProp = new Properties();
		String prefixMain = ConfigUtils.PROPERTY_PREFIX_CHANNEL + Model.CHANNEL_MAIN + ".";
		defaultChanProp.put(prefixMain + TedProperty.CHANNEL_WORKERS_COUNT, "5");
		defaultChanProp.put(prefixMain + TedProperty.CHANNEL_TASK_BUFFER, "100");
		String prefixSystem = ConfigUtils.PROPERTY_PREFIX_CHANNEL + Model.CHANNEL_SYSTEM + ".";
		defaultChanProp.put(prefixSystem + TedProperty.CHANNEL_WORKERS_COUNT, "2");
		defaultChanProp.put(prefixSystem + TedProperty.CHANNEL_TASK_BUFFER, "2000");
		ConfigUtils.readTedProperties(context.config, defaultChanProp);
		ConfigUtils.readTedProperties(context.config, properties);

		// Create channels
		for (String channel : context.config.channelMap().keySet()) {
			context.registry.registerChannel(channel, context.config.channelMap().get(channel));
		}
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
		driverExecutor = context.executors.createSchedulerExecutor(tedNamePrefix + "Driver-");;
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
		maintenanceExecutor = context.executors.createSchedulerExecutor(tedNamePrefix + "Maint-");
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
		long tillMs = startMs + (timeoutMs > 0 ? timeoutMs : 20 * 1000) - 100; // 100ms for finishing rest tasks
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
		//pools.put("(stats)", statsEventExecutor);
		for (Channel channel : context.registry.getChannels()) {
			pools.put(channel.name, channel.workers);
		}
		boolean interrupt = false;
		for (String poolId : pools.keySet()) {
			ExecutorService pool = pools.get(poolId);
			try {
				if (!pool.awaitTermination(Math.max(tillMs - System.currentTimeMillis(), 0L), TimeUnit.MILLISECONDS)) {
					if (pool instanceof ChannelThreadPoolExecutor) {
						((ChannelThreadPoolExecutor)pool).handleWorkingTasksOnShutdown();
					}
					logger.warn("WorkerPool {} did not terminated successfully", poolId);
				}
			} catch (InterruptedException e) {
				interrupt = true;
			}
		}
		statsEventExecutor.shutdown();
		if (interrupt)
			Thread.currentThread().interrupt(); // Preserve interrupt status (??? see ThreadPoolExecutor javadoc)
		isStartedFlag.set(false);
		logger.info("TED driver shutdown in {}ms", System.currentTimeMillis() - startMs);
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
	// if batchTaskName is null - will take from taskConfiguration
	public Long createBatch(String batchTaskName, String data, String key1, String key2, List<TedTask> tedTasks) {
		if (tedTasks == null || tedTasks.isEmpty())
			return null;
		if (batchTaskName == null) {
			throw new IllegalStateException("batchTaskName is required!");
		}
		TaskConfig batchTC = context.registry.getTaskConfig(batchTaskName);
		if (batchTC == null)
			throw new IllegalArgumentException("Batch task '" + batchTaskName + "' is not known for TED");

		Long batchId = context.tedDao.createTaskPostponed(batchTC.taskName, Model.CHANNEL_BATCH, data, key1, key2, 30 * 60);
		createTasksBulk(tedTasks, batchId);
		context.tedDao.setStatusPostponed(batchId, TedStatus.NEW, Model.BATCH_MSG, new Date());

		return batchId;
	}

	public Long createEvent(String taskName, String queueId, String data, String key2) {
		return context.eventQueueManager.createEvent(taskName, queueId, data, key2);
	}

	public Long createEventAndTryExecute(String taskName, String queueId, String data, String key2) {
		return context.eventQueueManager.createEventAndTryExecute(taskName, queueId, data, key2);
	}

	public Long sendNotification(String taskName, String data) {
		return context.notificationManager.sendNotification(taskName, data);
	}

	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory) {
		FieldValidator.validateTaskName(taskName);
		context.registry.registerTaskConfig(taskName, tedProcessorFactory);
	}

//	public void registerTaskConfig(String taskName, TedPackProcessorFactory tedPackProcessorFactory) {
//		FieldValidator.validateTaskName(taskName);
//		context.registry.registerTaskConfig(taskName, tedPackProcessorFactory);
//	}

	public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, Integer workTimeoutInMinutes, TedRetryScheduler retryScheduler, String channel) {
		FieldValidator.validateTaskName(taskName);
		context.registry.registerTaskConfig(taskName, tedProcessorFactory, null, workTimeoutInMinutes, retryScheduler, channel);
	}

	public void registerChannel(String channel, int workerCount, int taskBufferSize) {
		context.registry.registerChannel(channel, workerCount, taskBufferSize);
	}

	public TedTask getTask(long taskId) {
		TaskRec taskRec = context.tedDao.getTask(taskId);
		return taskRec == null ? null : taskRec.getTedTask();
	}

	public PrimeInstance prime() {
		return context.prime;
	}

	public void setMetricsRegistry(TedMetricsEvents metricsRegistry){
		context.stats.setMetricsRegistry(metricsRegistry);
	}


	//
	// package scoped - for tests and ted-ext only
	//
	TedContext getContext() {
		return context;
	}
}
