package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedDbType;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedRetryScheduler;
import ted.driver.Ted.TedStatus;
import ted.driver.TedDriverApi.TedDriverConfig;
import ted.driver.TedDriverApi.TedTaskConfig;
import ted.driver.TedTask;
import ted.driver.sys.ConfigUtils.TedConfig;
import ted.driver.sys.ConfigUtils.TedProperty;
import ted.driver.sys.Executors.ChannelThreadPoolExecutor;
import ted.driver.sys.Executors.TedRunnable;
import ted.driver.sys.Model.FieldValidator;
import ted.driver.sys.Model.TaskParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.Model.TedTaskImpl;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.Registry.TaskConfig;
import ted.driver.sys.SqlUtils.DbType;
import ted.driver.sys.TedDao.SetTaskStatus;
import ted.driver.sys.Trash.TedMetricsEvents;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static ted.driver.sys.MiscUtils.asList;

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
    private static final Logger logger = LoggerFactory.getLogger(TedDriverImpl.class);

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
        MaintenanceManager maintenanceManager;
        Executors executors;
        RetryConfig retryConfig;
        QuickCheck quickCheck;
        PrimeInstance prime;
        EventQueueManager eventQueueManager;
        BatchWaitManager batchWaitManager;
        NotificationManager notificationManager;
        CommandManager commandManager;
        Stats stats;
    }

    final DataSource dataSource;
    private final TedContext context;
    final AtomicBoolean isStartedFlag = new AtomicBoolean(false);
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

        context = new TedContext();
        context.tedDriver = this;
        context.config = new TedConfig(system);
        context.executors = new Executors(context);

        statsEventExecutor = context.executors.createExecutor(tedNamePrefix + "Stats" , 1, 2000);
        context.stats = new Stats(statsEventExecutor);

        ConfigUtils.readDriverProperties(context.config, properties);
        String tableName = context.config.tableName();
        String schemaName = context.config.schemaName();
        switch (dbType) {
            case POSTGRES:
                TedDaoPostgres pg = new TedDaoPostgres(system, dataSource, context.stats, schemaName, tableName);
                context.tedDao = pg;
                context.tedDaoExt = pg;
                break;
            case ORACLE:
                context.tedDao = new TedDaoOracle(system, dataSource, context.stats, schemaName, tableName);
                context.tedDaoExt = new TedDaoExtNA("Oracle");
                break;
            case MYSQL:
                context.tedDao = new TedDaoMysql(system, dataSource, context.stats, schemaName, tableName);
                context.tedDaoExt = new TedDaoExtNA("MySql");
                break;
            case HSQLDB:
                context.tedDao = new TedDaoHsqldb(system, dataSource, context.stats, schemaName, tableName);
                context.tedDaoExt = new TedDaoExtNA("HSQLDB");
                break;
            default: throw new IllegalStateException("Invalid case " + dbType);
        }
        context.registry = new Registry(context);
        context.taskManager = new TaskManager(context);
        context.maintenanceManager = new MaintenanceManager(context);
        context.retryConfig = new RetryConfig(context);
        context.quickCheck = new QuickCheck(context);
        context.prime = new PrimeInstance(context);
        context.eventQueueManager = new EventQueueManager(context);
        context.batchWaitManager = new BatchWaitManager(context);
        context.notificationManager = new NotificationManager(context);
        context.commandManager = new CommandManager(context);

        // read task and channel properties (e.g. from ted.properties)
        // default MAIN channel configuration: 5/100. Can be overwritten by [properties]
        //
        Properties defaultChanProp = new Properties();
        String prefixMain = ConfigUtils.PROPERTY_PREFIX_CHANNEL + Model.CHANNEL_MAIN + ".";
        defaultChanProp.put(prefixMain + TedProperty.CHANNEL_WORKERS_COUNT, "5");
        defaultChanProp.put(prefixMain + TedProperty.CHANNEL_TASK_BUFFER, "100");
        String prefixSystem = ConfigUtils.PROPERTY_PREFIX_CHANNEL + Model.CHANNEL_SYSTEM + ".";
        defaultChanProp.put(prefixSystem + TedProperty.CHANNEL_WORKERS_COUNT, "2");
        defaultChanProp.put(prefixSystem + TedProperty.CHANNEL_TASK_BUFFER, "2000");
        ConfigUtils.readTaskAndChannelProperties(context.config, defaultChanProp);
        ConfigUtils.readTaskAndChannelProperties(context.config, properties);

        if (context.config.rebuildIndexIntervalHours() > 0 && context.tedDao.getDbType() != DbType.POSTGRES) {
            logger.warn("Parameter ted.maintenance.rebuildIndexIntervalHours is only for PostgreSql");
            context.config.setRebuildIndexIntervalHours(0);
        }

        // Create channels
        for (String channel : context.config.channelMap().keySet()) {
            context.registry.registerChannel(channel, context.config.channelMap().get(channel));
        }

        // if something is wrong with batch updates, then can disable them by config. Will remove this later.
        if (context.config.isDisabledSqlBatchUpdate()) {
            context.taskManager.enableResultStatusPacking(false);
            if (context.tedDao instanceof TedDaoAbstract)
                ((TedDaoAbstract) context.tedDao).setSqlBatchUpdateDisabled(true);
        }

        context.commandManager.initCommandTask();
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
        driverExecutor = context.executors.createSchedulerExecutor(tedNamePrefix + "Driver-");
        driverExecutor.scheduleAtFixedRate(() -> {
            if (context.config.isDisabledProcessing()) {
                return;
            }
            try {
                context.taskManager.flushStatuses();
                context.quickCheck.quickCheck();
                Thread.sleep(30); // cool down, just in case
            } catch (Throwable e) {
                logger.error("Error while executing driver task", e);
            }
        }, context.config.initDelayMs(), context.config.intervalDriverMs(), TimeUnit.MILLISECONDS);

        // maintenance tasks processor
        maintenanceExecutor = context.executors.createSchedulerExecutor(tedNamePrefix + "Maint-");
        maintenanceExecutor.scheduleAtFixedRate(() -> {
            if (context.config.isDisabledProcessing()) {
                logger.info("Processing is disabled");
                return;
            }
            try {
                context.maintenanceManager.processMaintenanceTasks();
            } catch (Throwable e) {
                logger.error("Error while executing maintenance tasks", e);
            }
        }, context.config.initDelayMs(), context.config.intervalMaintenanceMs(), TimeUnit.MILLISECONDS);
    }

    public void shutdown(long timeoutMs) {
        long startMs = System.currentTimeMillis();
        long tillMs = startMs + (timeoutMs > 0 ? timeoutMs : 20 * 1000) - 100; // 100ms for finishing rest tasks
        if (!isStartedFlag.get()) {
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
        List<TedRunnable> tasksToReturn = new ArrayList<>();
        for (Channel channel : context.registry.getChannels()) {
            List<Runnable> chanTasks = channel.workers.shutdownNow();
            for (Runnable r : chanTasks) {
                tasksToReturn.add((TedRunnable) r);
            }
        }

        // return back not started tasks to status NEW
        List<SetTaskStatus> statuses = new ArrayList<>();
        for (TedRunnable tedr : tasksToReturn) {
            TaskRec task = tedr.getTask();
            logger.info("return back task {} (taskId={}) to status NEW", task.name, task.taskId);
            statuses.add(new SetTaskStatus(task.taskId, TedStatus.NEW, "return on shutdown", new Date()));
        }
        context.tedDao.setStatuses(statuses);

        context.taskManager.enableResultStatusPacking(false);
        context.taskManager.flushStatuses();

        // wait for finish
        logger.debug("waiting for finish TED tasks...");
        Map<String, ExecutorService> pools = new LinkedHashMap<>();
        pools.put("(driver)", driverExecutor);
        pools.put("(maintenance)", maintenanceExecutor);
        for (Channel channel : context.registry.getChannels()) {
            pools.put(channel.name, channel.workers);
        }
        boolean interrupt = false;
        for (Entry<String, ExecutorService> entry : pools.entrySet()) {
            String poolId = entry.getKey();
            ExecutorService pool = entry.getValue();
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
            Thread.currentThread().interrupt(); // Preserve interrupt status (see ThreadPoolExecutor javadoc)
        isStartedFlag.set(false);
        logger.info("TED driver shutdown in {}ms", System.currentTimeMillis() - startMs);
    }

    /*
     * "public" for TedDriver only
     */

    public Long createTask(String taskName, String data, String key1, String key2, Long batchId, String channel, Connection conn) {
        FieldValidator.validateTaskData(data);
        FieldValidator.validateTaskKey1(key1);
        FieldValidator.validateTaskKey2(key2);
        if (channel == null) {
            TaskConfig tc = context.registry.getTaskConfig(taskName);
            if (tc == null)
                throw new IllegalArgumentException("Task '" + taskName + "' is not known for TED");
            channel = tc.channel;
        } else {
            Channel ch = context.registry.getChannel(channel);
            if (ch == null)
                throw new IllegalArgumentException("Channel '" + channel + "' is not known for TED");
        }
        return context.tedDao.createTask(taskName, channel, data, key1, key2, batchId, conn);
    }

    Long createTask(String taskName, String data, String key1, String key2) {
        return createTask(taskName, data, key1, key2, null, null, null);
    }

    public Long createTaskPostponed(String taskName, String data, String key1, String key2, int postponeSec, String channel, Connection conn) {
        FieldValidator.validateTaskData(data);
        FieldValidator.validateTaskKey1(key1);
        FieldValidator.validateTaskKey2(key2);
        if (channel == null) {
            TaskConfig tc = context.registry.getTaskConfig(taskName);
            if (tc == null)
                throw new IllegalArgumentException("Task '" + taskName + "' is not known for TED");
            channel = tc.channel;
        } else {
            Channel ch = context.registry.getChannel(channel);
            if (ch == null)
                throw new IllegalArgumentException("Channel '" + channel + "' is not known for TED");
        }
        return context.tedDao.createTaskPostponed(taskName, channel, data, key1, key2, postponeSec, conn);
    }

    // create task and execute it
    // (or add task to execution queue - channel)
    public Long createAndExecuteTask(String taskName, String data, String key1, String key2, boolean inChannel, Connection conn) {
        FieldValidator.validateTaskData(data);
        FieldValidator.validateTaskKey1(key1);
        FieldValidator.validateTaskKey2(key2);
        TaskConfig tc = context.registry.getTaskConfig(taskName);
        if (tc == null)
            throw new IllegalArgumentException("Task '" + taskName + "' is not known for TED");
        Long taskId = context.tedDao.createTaskWithWorkStatus(taskName, tc.channel, data, key1, key2, conn);
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
            context.taskManager.processTask(taskRec);
        }
        return taskId;
    }

    public List<Long> createTasksBulk(List<TedTask> tedTasks, Long batchId, Connection connection) {
        if (tedTasks == null || tedTasks.isEmpty())
            return Collections.emptyList();

        List<TaskParam> taskParams = new ArrayList<>();
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
            tp.batchId = batchId;
            taskParams.add(tp);
        }
        return context.tedDao.createTasksBulk(taskParams, connection);
    }

    // create tasks by list and batch task for them. return batch taskId
    // if batchTaskName is null - will take from taskConfiguration
    public Long createBatch(String batchTaskName, String data, String key1, String key2, List<TedTask> tedTasks, Connection connection) {
        if (tedTasks == null || tedTasks.isEmpty())
            return null;
        if (batchTaskName == null) {
            throw new IllegalStateException("batchTaskName is required!");
        }
        TaskConfig batchTC = context.registry.getTaskConfig(batchTaskName);
        if (batchTC == null)
            throw new IllegalArgumentException("Batch task '" + batchTaskName + "' is not known for TED");

        Long batchId = context.tedDao.createTaskPostponed(batchTC.taskName, Model.CHANNEL_BATCH, data, key1, key2, 30 * 60, connection);
        createTasksBulk(tedTasks, batchId, connection);
        Date nextTs = new Date(System.currentTimeMillis() + 2 * 1000); // postpone for 2 sec, just in case
        context.tedDao.setStatuses(asList(new SetTaskStatus(batchId, TedStatus.NEW, Model.BATCH_MSG, nextTs)), connection);

        return batchId;
    }

    public Long createEvent(String taskName, String queueId, String data, String key2, int postponeSec) {
        return context.eventQueueManager.createEvent(taskName, queueId, data, key2, postponeSec);
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

    public void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory, Integer workTimeoutInMinutes, TedRetryScheduler retryScheduler, String channel) {
        FieldValidator.validateTaskName(taskName);
        context.registry.registerTaskConfig(taskName, tedProcessorFactory, workTimeoutInMinutes, retryScheduler, channel);
    }

    public void registerChannel(String channel, int workerCount, int taskBufferSize) {
        context.registry.registerChannel(channel, workerCount, taskBufferSize);
    }

    public TedTask getTask(long taskId) {
        TaskRec taskRec = context.tedDao.getTask(taskId);
        if (taskRec == null)
            return null;
        TedTaskImpl task = (TedTaskImpl) taskRec.getTedTask();
        boolean isLastTry = isTaskLastTry(task);
        task.setIsLastTry(isLastTry);
        return task;
    }

    public PrimeInstance prime() {
        return context.prime;
    }

    public void setMetricsRegistry(TedMetricsEvents metricsRegistry){
        context.stats.setMetricsRegistry(metricsRegistry);
    }

    public TedDriverConfig getTedDriverConfig() {
        return taskName -> {
            TaskConfig taskConfig = context.registry.getTaskConfig(taskName);
            if (taskConfig == null)
                return null;
            return taskConfig.api;
        };
    }

    public TedTask newTedTask(String taskName, String data, String key1, String key2) {
        return new TedTaskImpl(null, taskName, key1, key2, data);
    }

    private boolean isTaskLastTry(TedTask task) {
        TedDriverConfig driverConfig = getTedDriverConfig();
        TedTaskConfig taskConfig = driverConfig.getTaskConfig(task.getName());
        TedRetryScheduler retryScheduler = taskConfig.getRetryScheduler();
        Date nextTs = retryScheduler.getNextRetryTime(task, task.getRetries() + 1, task.getStartTs());
        return nextTs == null;
    }


    //
    // package scoped - for tests and ted-ext only
    //
    TedContext getContext() {
        return context;
    }
}
