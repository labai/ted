package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedRetryScheduler;
import ted.driver.TedDriverApi.TedTaskConfig;
import ted.driver.sys.ConfigUtils.TedProperty;
import ted.driver.sys.Model.FieldValidator;
import ted.driver.sys.RetryConfig.PeriodPatternRetryScheduler;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
    private final static int MAX_CHANNELS = 10; // limit user channel count to keep performance
    private final static int CHANNEL_EXTRA_SIZE = 500; // queue size increase - reserved for createAndStart tasks

    private final TedContext context;

    private Map<String, TaskConfig> tasks = new ConcurrentHashMap<>();
    private Map<String, Channel> channels = new ConcurrentHashMap<>();

    class Channel {
        final String name;
        private final int workerCount;
        final int taskBufferSize;
        final boolean primeOnly;
        final ThreadPoolExecutor workers;
        private int slowStartCount = TaskManager.SLOW_START_COUNT; // after no task period will start to slowly increase count of tasks to select (purpose is to do some balance between nodes). But for pack processing tasks this behavior is wrong. Will be 3 by default, but if exists task with pack processing, then use 1000 (max)

        Channel(String tedNamePrefix, String name, int workerCount, int taskBufferSize, boolean primeOnly) {
            this.name = name;
            this.workerCount = workerCount;
            this.taskBufferSize = taskBufferSize;
            this.primeOnly = primeOnly;
            this.workers = context.executors.createChannelExecutor(name, tedNamePrefix + "-" + name, workerCount, taskBufferSize + workerCount + CHANNEL_EXTRA_SIZE);
        }

        int getSlowStartCount() { return this.slowStartCount; }

        //
        int getQueueRemainingCapacity() {
            return workers.getQueue().remainingCapacity() - workerCount - CHANNEL_EXTRA_SIZE;
        }
    }

    static class TaskConfig {
        final String taskName;
        final TedProcessorFactory tedProcessorFactory;
        final int workTimeoutMinutes;
        final String channel;
        final TedRetryScheduler retryScheduler;
        final int batchTimeoutMinutes;
        final String shortLogName; // 5 letters name for threadName

        public TaskConfig(String taskName, TedProcessorFactory tedProcessorFactory,
            int workTimeoutMinutes, TedRetryScheduler retryScheduler, String channel,
            int batchTimeoutMinutes) {
            this.taskName = taskName;
            this.tedProcessorFactory = tedProcessorFactory;
            this.workTimeoutMinutes = Math.max(workTimeoutMinutes, 1); // timeout, less than 1 minute, is invalid, as process will check timeouts only >= 1 min
            this.retryScheduler = retryScheduler;
            this.channel = channel == null ? Model.CHANNEL_MAIN : channel;
            this.batchTimeoutMinutes = batchTimeoutMinutes;
            this.shortLogName = makeShortName(taskName);
        }

        // api for public
        final TedTaskConfig api = new TedTaskConfig() {
            @Override public TedRetryScheduler getRetryScheduler() { return retryScheduler; }
        };
    }

    static String makeShortName(String taskName) {
        final int prefixLen = 2, hashLen = 3;
        if (taskName.length() <= prefixLen + hashLen)
            return taskName.toUpperCase();
        String prefix = (taskName.replace("-", "").replace("_", "")+ "XX").substring(0, prefixLen);
        String hash = "XXX" + Integer.toString(Math.abs(taskName.hashCode() % Integer.MAX_VALUE), 36);
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


    void registerTaskConfig(String taskName, TedProcessorFactory tedProcessorFactory,
        Integer workTimeoutInMinutes, TedRetryScheduler retryScheduler, String channel) {

        if (tasks.containsKey(taskName)) {
            logger.warn("Task '" + taskName + "' already exists in registry, skip to register new one");
            return;
        }

        // overwrite parameters from config
        Properties shortProp = context.config.taskMap().get(taskName);
        workTimeoutInMinutes = ConfigUtils.getInteger(shortProp, TedProperty.TASK_TIMEOUT_MINUTES, workTimeoutInMinutes);
        channel = ConfigUtils.getString(shortProp, TedProperty.TASK_CHANNEL, channel);

        // assign defaults if nulls
        if (workTimeoutInMinutes == null)
            workTimeoutInMinutes = context.config.defaultTaskTimeoutMn();
        //if (retryPattern == null)
        //	retryPattern = context.config.defaultRetryPauses();

        int batchTimeoutInMinutes = ConfigUtils.getInteger(shortProp, TedProperty.TASK_BATCH_TIMEOUT_MINUTES, context.config.defaultBatchTaskTimeoutMn());

        if (channel == null)
            channel = Model.CHANNEL_MAIN;
        Channel channelConfig = getChannel(channel);
        if (channelConfig == null)
            throw new IllegalArgumentException("Channel '" + channel + "' does not exists");

        if (retryScheduler == null) {
            String retryPattern = ConfigUtils.getString(shortProp, TedProperty.TASK_RETRY_PAUSES, context.config.defaultRetryPauses());
            retryScheduler = new PeriodPatternRetryScheduler(retryPattern);
        }

        if (Model.nonTaskChannels.contains(channel))
            throw new IllegalStateException("Channel '"+ channel +"' cannot be assigned to regular task - is is reserved for Ted");

        TaskConfig ttc = new TaskConfig(taskName, tedProcessorFactory,
            workTimeoutInMinutes, retryScheduler, channel,
            // taskType, batchTask,
            batchTimeoutInMinutes);
        tasks.put(taskName, ttc);
        loggerConfig.info("Register task {} (channel={} timeoutMinutes={} logid={} {})", ttc.taskName, ttc.channel, ttc.workTimeoutMinutes, ttc.shortLogName, (batchTimeoutInMinutes>0?" batchTimeoutInMinutes="+batchTimeoutInMinutes:""));
    }

    public TaskConfig getTaskConfig(String taskName) {
        return tasks.get(taskName);
    }

    //
    // channels
    // (create internally in TedDriverImpl)
    //
    void registerChannel(String channel, Properties shortProperties) {
        int workerCount = ConfigUtils.getInteger(shortProperties, TedProperty.CHANNEL_WORKERS_COUNT, 5);
        int bufferSize = ConfigUtils.getInteger(shortProperties, TedProperty.CHANNEL_TASK_BUFFER, 200);
        boolean primeOnly = "yes".equals(ConfigUtils.getString(shortProperties, TedProperty.CHANNEL_PRIME_ONLY, "no"));

        registerChannel(channel, workerCount, bufferSize, primeOnly);
    }
    void registerChannel(String channel, int workerCount, int bufferSize) {
        registerChannel(channel, workerCount, bufferSize, false);
    }

    void registerChannel(String channel, int workerCount, int bufferSize, boolean primeOnly) {
        if (workerCount < 1 || workerCount > 1000)
            throw new IllegalArgumentException("Worker count must be number between 1 and 1000, channel=" + channel);
        FieldValidator.validateTaskChannel(channel);
        if (channels.containsKey(channel)) {
            logger.warn("Channel '" + channel + "' already exists in registry, skip to register new one");
            return;
        }
        if (! Model.nonTaskChannels.contains(channel)) {
            Set<String> userChannels = new HashSet<>(channels.keySet());
            userChannels.removeAll(Model.nonTaskChannels);
            if (userChannels.size() >= MAX_CHANNELS)
                throw new IllegalStateException("(TED) Exceeded maximum number of channels (" + MAX_CHANNELS + ")!");
        }

        Channel ochan = new Channel(context.tedDriver.tedNamePrefix, channel, workerCount, bufferSize, primeOnly);
        channels.put(channel, ochan);
        loggerConfig.info("Register channel {} (workerCount={} taskBufferSize={})", ochan.name, ochan.workerCount, ochan.taskBufferSize);
    }

    public Channel getChannel(String name) {
        return channels.get(name);
    }

    Channel getChannelOrMain(String name) {
        Channel channel = channels.get(name);
        if (channel == null)
            channel = channels.get(Model.CHANNEL_MAIN);
        return channel;
    }

    Channel getChannelOrSystem(String name) {
        Channel channel = channels.get(name);
        if (channel == null)
            channel = channels.get(Model.CHANNEL_SYSTEM);
        return channel;
    }

    Collection<Channel> getChannels() {
        return Collections.unmodifiableCollection(channels.values());
    }

}
