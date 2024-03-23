package ted.driver.sys;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedStatus;
import ted.driver.TedResult;
import ted.driver.sys.Executors.TedRunnable;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.Model.TedTaskImpl;
import ted.driver.sys.QuickCheck.GetWaitChannelsResult;
import ted.driver.sys.QuickCheck.Tick;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.Registry.TaskConfig;
import ted.driver.sys.TedDao.SetTaskStatus;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static ted.driver.sys.MiscUtils.asList;

/**
 * @author Augustus
 *         created on 2016.09.19
 *
 * for internal usage only!!!
 */
class TaskManager {
    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

    static final int MAX_TASK_COUNT = 1000;
    static final int LIMIT_TOTAL_WAIT_TASKS = 20000; // max waiting tasks (aim to don't consume all memory)
    private static final long UNKNOWN_TASK_POSTPONE_MS = 120 * 1000L; // 2 min
    private static final long UNKNOWN_TASK_CANCEL_AFTER_MS = 24 * 3600 * 1000L;

    private final TedContext context;
    private final TaskStatusManager taskStatusManager;

    private static class ChannelWorkContext {
        final String channelName;
        int lastGotCount = 0;
        int nextPortion = 0;
        boolean foundTask = false;
        ChannelWorkContext(String channelName) {
            this.channelName = channelName;
        }
    }

    private final Map<String, ChannelWorkContext> channelContextMap = new HashMap<>();


    TaskManager(TedContext context) {
        this.context = context;
        this.taskStatusManager = new TaskStatusManager(context.tedDao);
    }

    void changeTaskStatusPostponed(long taskId, TedStatus status, String msg, Date nextTs){
        taskStatusManager.saveTaskStatus(taskId, status, msg, nextTs, null);

    }
    void changeTaskStatus(long taskId, TedStatus status, String msg, long startTs){
        taskStatusManager.saveTaskStatus(taskId, status, msg, null, new Date(startTs));
    }

    // flush statuses
    public void flushStatuses() {
        taskStatusManager.flush();
    }

    // enable/disable tasks status packing/batching
    public void enableResultStatusPacking(boolean enable) {
        taskStatusManager.setIsPackingEnabled(enable);
    }

    // tests only
    void processChannelTasks(Tick tick) {
        List<GetWaitChannelsResult> waitChannelsList = context.tedDao.getWaitChannels(tick);
        List<String> channels = waitChannelsList.stream()
            .filter(it -> ! Model.nonTaskChannels.contains(it.channel) )
            .map(it -> it.channel)
            .collect(Collectors.toList());
        processChannelTasks(channels, tick);
    }


    // process TED tasks
    // return flag, was any of channels fully loaded
    //
    boolean processChannelTasks(List<String> waitChannelsList, Tick tick) {
        int totalProcessing = calcWaitingTaskCountInAllChannels();
        if (totalProcessing >= LIMIT_TOTAL_WAIT_TASKS) {
            logger.warn("Total size of waiting tasks ({}) already exceeded limit ({}), skip this iteration", totalProcessing, LIMIT_TOTAL_WAIT_TASKS);
            return false;
        }

        if (waitChannelsList.isEmpty()) {
            logger.trace("no wait tasks");
            return false;
        }

        // check and log for unknown channels, remove special channels
        for (String waitChan : waitChannelsList) {
            if (context.registry.getChannel(waitChan) == null)
                logger.warn("Channel '{}' is not configured, but exists a waiting task with that channel", waitChan);
        }

        Collection<Channel> channels = context.registry.getChannels();
        for (Channel channel : channels) {
            ChannelWorkContext wc = channelContextMap.get(channel.name);
            if (wc == null) {
                wc = new ChannelWorkContext(channel.name);
                channelContextMap.put(channel.name, wc);
            }
            wc.foundTask = waitChannelsList.contains(channel.name);
            if (!wc.foundTask)
                continue;
            // calc max size of tasks we can take now
            wc.nextPortion = calcChannelBufferFree(channel);
        }

        // fill channels for request
        Map<String, Integer> channelSizes = new HashMap<>();
        for (ChannelWorkContext wc : channelContextMap.values()) {
            if (wc.nextPortion > 0 && wc.foundTask)
                channelSizes.put(wc.channelName, wc.nextPortion);
        }

        // select tasks from db
        //
        List<TaskRec> tasks = context.tedDao.reserveTaskPortion(channelSizes, tick);

        // calc stats
        //
        calcChannelsStats(tasks);

        if (tasks.isEmpty()) {
            logger.debug("no tasks (full check): {}", channelSizes.keySet());
            return false;
        }

        sendTaskListToChannels(tasks);

        boolean wasAnyFullLoaded = false;
        for (ChannelWorkContext wc : channelContextMap.values()) {
            if (wc.lastGotCount > 0 && wc.lastGotCount == wc.nextPortion) {
                wasAnyFullLoaded = true;
                break;
            }
        }
        return wasAnyFullLoaded;
    }


    // will send task to their channels for execution
    void sendTaskListToChannels(List<TaskRec> tasks) {

        // group by name
        Map<String, List<TaskRec>> grouped = tasks.stream()
            .collect(Collectors.groupingBy(it -> it.name));

        // execute
        //
        for (String taskName : grouped.keySet()) {
            final List<TaskRec> taskList = grouped.get(taskName);
            TaskRec trec1 = taskList.get(0);
            Channel channel = context.registry.getChannel(trec1.channel);
            if (channel == null) { // should never happen
                logger.warn("Task channel '{}' not exists. Use channel MAIN (task={} taskId={})", trec1.channel, trec1.name, trec1.taskId);
                channel = context.registry.getChannel(Model.CHANNEL_MAIN);
            }
            TaskConfig taskConfig = context.registry.getTaskConfig(trec1.name);
            if (taskConfig == null) {
                handleUnknownTasks(taskList);
                continue;
            }

            for (final TaskRec taskRec : taskList) {
                logger.debug("got task: {}", taskRec);
                context.stats.metrics.loadTask(taskRec.taskId, taskRec.name, taskRec.channel);
                channel.workers.execute(new TedRunnable(taskRec) {
                    @Override
                    public void run() {
                        processTask(taskRec);
                    }
                });
            }
        }
    }


    // can be called from eventQueue also
    void handleUnknownTasks(List<TaskRec> taskRecList) {
        long nowMs = System.currentTimeMillis();
        for (TaskRec taskRec : taskRecList) {
            if (taskRec.createTs.getTime() < nowMs - UNKNOWN_TASK_CANCEL_AFTER_MS) {
                logger.warn("Task is unknown and was not processed during 24 hours, mark as error: {}", taskRec);
                context.tedDao.setStatus(taskRec.taskId, TedStatus.ERROR, "unknown task");
            } else {
                logger.warn("Task is unknown, mark as new, postpone: {}", taskRec);
                context.tedDao.setStatusPostponed(taskRec.taskId, TedStatus.NEW, "unknown task. postpone", new Date(nowMs + UNKNOWN_TASK_POSTPONE_MS));
            }
        }
    }

    void processTask(TaskRec taskRec) {

        long startMs = System.currentTimeMillis();

        context.stats.metrics.startTask(taskRec.taskId, taskRec.name, taskRec.channel);

        String threadName = Thread.currentThread().getName();

        TedResult result = null;
        try {

            TaskConfig taskConfig = context.registry.getTaskConfig(taskRec.name);

            Thread.currentThread().setName(threadName + "-" + taskConfig.shortLogName + "-" + taskRec.taskId);

            // process
            //
            TedTaskImpl task = (TedTaskImpl) taskRec.getTedTask();
            Date nextRetryTm = taskConfig.retryScheduler.getNextRetryTime(task, task.getRetries() + 1, task.getStartTs());
            task.setIsLastTry(nextRetryTm == null);

            TedProcessor processor = taskConfig.tedProcessorFactory.getProcessor(taskRec.name);
            result = processor.process(task);

            // check results
            //
            if (result == null) {
                changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "result is null", startMs);
            } else if (result.status() == TedStatus.RETRY) {
                if (nextRetryTm == null) {
                    changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "max retries. " + result.message(), startMs);
                } else {
                    changeTaskStatusPostponed(taskRec.taskId, result.status(), result.message(), nextRetryTm);
                }
            } else if (result.status() == TedStatus.DONE || result.status() == TedStatus.ERROR) {
                changeTaskStatus(taskRec.taskId, result.status(), result.message(), startMs);
            } else {
                changeTaskStatus(taskRec.taskId, TedStatus.ERROR, "invalid result status: " + result.status(), startMs);
            }

        } catch (Throwable e) {
            String msg = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
            logger.info("Unhandled exception while calling processor for task '{}': {}", taskRec.name, msg);
            taskExceptionLogger.error("Unhandled exception while calling processor for task '" + taskRec.name + "'", e);
            try {
                result = TedResult.error("Catch: " + msg);
                changeTaskStatus(taskRec.taskId, TedStatus.ERROR, result.message(), startMs);
            } catch (Exception e1) {
                logger.warn("Unhandled exception while handling exception for task '{}', statuses will be not changed: {}", taskRec.name, e1.getMessage());
            }
            if (e instanceof ThreadDeath)
                throw e;
        } finally {
            Thread.currentThread().setName(threadName);
        }

        context.stats.metrics.finishTask(taskRec.taskId, taskRec.name, taskRec.channel, (result == null ? TedStatus.ERROR : result.status()), (int)(System.currentTimeMillis() - startMs));
    }


    int calcChannelBufferFree(Channel channel) {
        int workerCount = channel.workers.getMaximumPoolSize();
        int queueRemain = channel.getQueueRemainingCapacity();
        int maxTask = workerCount - channel.workers.getActiveCount() + queueRemain;
        maxTask = Math.max(Math.min(maxTask, MAX_TASK_COUNT), 0);
        logger.debug(channel.name + " max_count=" + maxTask + " (workerCount=" + workerCount + " activeCount=" + channel.workers.getActiveCount() + " remainingCapacity=" + queueRemain + " maxQueue=" + channel.taskBufferSize + ")");
        if (maxTask == 0) {
            logger.debug("Channel {} queue is full", channel.name);
        }
        return maxTask;
    }


    // refresh channels work info. is not thread safe
    private void calcChannelsStats(List<TaskRec> tasks) {
        for (ChannelWorkContext wc : channelContextMap.values()) {
            wc.lastGotCount = 0;
        }
        for (TaskRec taskRec : tasks) {
            ChannelWorkContext wc = channelContextMap.get(taskRec.channel);
            if (wc == null) {
                wc = new ChannelWorkContext(taskRec.channel);
                channelContextMap.put(taskRec.channel, wc);
            }
            wc.lastGotCount++;
        }
    }

    int calcWaitingTaskCountInAllChannels() {
        int sum = 0;
        for (Channel channel : context.registry.getChannels()) {
            Queue<Runnable> queue = channel.workers.getQueue();
            try {
                for (Runnable r : queue) {
                    TedRunnable tr = (TedRunnable) r;
                    sum = sum + tr.getTaskCount();
                }
            } catch (ConcurrentModificationException e) {
                // should not happen as LinkedBlockingQueue should not raise this exception
                continue;
            }
        }
        return sum;
    }

    /*
     * will gather task results till next tick
     */
    static class TaskStatusManager {
        private final TedDao tedDao;

        private final List<SetTaskStatus> resultsToSave = new ArrayList<>();

        private final AtomicBoolean isPackingEnabled = new AtomicBoolean(true);


        TaskStatusManager(TedDao tedDao) {
            this.tedDao = tedDao;
        }

        void saveTaskStatus(long taskId, TedStatus status, String msg, Date nextTs, Date startTs){
            SetTaskStatus sta = new SetTaskStatus(taskId, status, msg, nextTs, startTs);
            if (isPackingEnabled.get() == true) {
                synchronized (resultsToSave) {
                    resultsToSave.add(sta);
                }
            } else {
                tedDao.setStatuses(asList(sta));
            }
        }

        void saveTaskStatus(long taskId, TedStatus status, String msg){
            saveTaskStatus(taskId, status, msg, null, null);
        }

        void flush() {
            List<SetTaskStatus> res = new ArrayList<>();
            synchronized (resultsToSave) {
                res.addAll(resultsToSave);
                resultsToSave.clear();
            }
            if (res.isEmpty())
                return;
            logger.debug("Flush {} statuses", res.size());
            tedDao.setStatuses(res);
        }

        public void setIsPackingEnabled(boolean enabled) {
            this.isPackingEnabled.set(enabled);
            if (enabled == false) {
                flush();
            }
        }
    }

}
