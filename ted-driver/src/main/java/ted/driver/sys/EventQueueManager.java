package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedStatus;
import ted.driver.TedResult;
import ted.driver.sys.Executors.TedRunnable;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.Model.TedTaskImpl;
import ted.driver.sys.QuickCheck.Tick;
import ted.driver.sys.Registry.Channel;
import ted.driver.sys.Registry.TaskConfig;
import ted.driver.sys.TedDao.SetTaskStatus;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ted.driver.sys.MiscUtils.asList;


/**
 * @author Augustus
 *         created on 2018.08.07
 *
 * for internal usage only!!!
 */
class EventQueueManager {
    private static final Logger logger = LoggerFactory.getLogger(EventQueueManager.class);
    private static final Logger taskExceptionLogger = LoggerFactory.getLogger("ted-task");

    private final TedContext context;
    private final TedDao tedDao;
    private final TedDaoExt tedDaoExt;

    public EventQueueManager(TedContext context) {
        this.context = context;
        this.tedDao = context.tedDao;
        this.tedDaoExt = context.tedDaoExt;
    }

    // channels - always TedEQ.
    // there will be "head" event with status NEW/RETRY, and may be tail events with status SLEEP.
    // heads uniqueness by key1 should be guaranteed by unique index.
    void processTedQueue() {
        int totalProcessing = context.taskManager.calcWaitingTaskCountInAllChannels();
        if (totalProcessing >= TaskManager.LIMIT_TOTAL_WAIT_TASKS) {
            logger.warn("Total size of waiting tasks ({}) already exceeded limit ({}), skip this iteration (2)", totalProcessing, TaskManager.LIMIT_TOTAL_WAIT_TASKS);
            return;
        }
        Channel channel = context.registry.getChannelOrMain(Model.CHANNEL_QUEUE);
        int maxTask = context.taskManager.calcChannelBufferFree(channel);
        maxTask = Math.min(maxTask, 50);
        Map<String, Integer> channelSizes = new HashMap<>();
        channelSizes.put(Model.CHANNEL_QUEUE, maxTask);
        List<TaskRec> heads = tedDao.reserveTaskPortion(channelSizes, new Tick(1));
        if (heads.isEmpty())
            return;

        for (final TaskRec head : heads) {
            logger.debug("exec eventQueue for '{}', headTaskId={}", head.key1, head.taskId);
            channel.workers.execute(new TedRunnable(head) {
                @Override
                public void run() {
                    processEventQueue(head);
                }
            });
        }
    }

    private SetTaskStatus makeResultSta(TaskRec event, TedResult result, long startMs) {
        if (result.status() == TedStatus.RETRY) {
            TaskConfig tc = context.registry.getTaskConfig(event.name);
            Date nextTm = tc.retryScheduler.getNextRetryTime(event.getTedTask(), event.retries + 1, event.startTs);
            return new SetTaskStatus(event.taskId, result.status(), result.message(), nextTm, new Date(startMs));
        } else {
            return new SetTaskStatus(event.taskId, result.status(), result.message(), null, new Date(startMs));
        }
    }

    // process events from queue each after other, until ERROR or RETRY will happen
    private void processEventQueue(final TaskRec head) {
        TaskConfig tc = context.registry.getTaskConfig(head.name);
        if (tc == null) {
            context.taskManager.handleUnknownTasks(asList(head));
            return;
        }

        long headStartMs = System.currentTimeMillis();
        TedResult headResult = processEvent(head);
        SetTaskStatus headResultSta = makeResultSta(head, headResult, headStartMs);

        SetTaskStatus lastUnsavedResultSta = null;
        // try to execute next events, while head is reserved. some events may be created while executing current
        if (headResult.status() == TedStatus.DONE) {
            outer:
            for (int i = 0; i < 10; i++) {
                List<TaskRec> events = tedDaoExt.eventQueueGetTail(head.key1);
                if (events.isEmpty())
                    break outer;
                for (TaskRec event : events) {
                    TaskConfig tc2 = context.registry.getTaskConfig(event.name);
                    if (tc2 == null)
                        break outer; // unknown task, leave it for later

                    long startMs = System.currentTimeMillis();
                    TedResult result = processEvent(event);

                    // DONE - final status, on which can continue with next event
                    if (result.status() == TedStatus.DONE) {
                        SetTaskStatus sta = makeResultSta(event, result, startMs);
                        tedDao.setStatuses(asList(sta));
                    } else {
                        lastUnsavedResultSta = makeResultSta(event, result, startMs);
                        break outer;
                    }
                }
            }
        }

        // first save head, otherwise unique index will fail
        final SetTaskStatus finalLastUnsavedResultSta = lastUnsavedResultSta;
        tedDaoExt.runInTx((conn) -> {
            try {
                tedDao.setStatuses(asList(headResultSta), conn);
                if (finalLastUnsavedResultSta != null) {
                    tedDao.setStatuses(asList(finalLastUnsavedResultSta), conn);
                }
            } catch (Throwable e) {
                logger.error("Error while finishing events queue execution", e);
            }
            return true;
        });
    }


    Long createEvent(String taskName, String queueId, String data, String key2, int postponeSec) {
        Long taskId = tedDaoExt.createEvent(taskName, queueId, data, key2);
        tedDaoExt.eventQueueMakeFirst(queueId, postponeSec);
        return taskId;
    }

    Long createEventAndTryExecute(String taskName, String queueId, String data, String key2) {
        long taskId = tedDaoExt.createEvent(taskName, queueId, data, key2);
        TaskRec task = tedDaoExt.eventQueueMakeFirst(queueId, 0);
        if (task != null && task.taskId == taskId) {
            TaskRec taskRec = tedDaoExt.eventQueueReserveTask(task.taskId);
            if (taskRec != null && taskRec.taskId == taskId) {
                processEventQueue(task);
            }
        }
        return taskId;
    }


    private TedResult processEvent(TaskRec taskRec1) {
        String threadName = Thread.currentThread().getName();
        logger.debug("Start to process event {}", taskRec1);
        TedResult result;
        try {
            TaskConfig taskConfig = context.registry.getTaskConfig(taskRec1.name);
            Thread.currentThread().setName(threadName + "-" + taskConfig.shortLogName + "-" + taskRec1.taskId);

            // process
            //
            TedTaskImpl task = (TedTaskImpl) taskRec1.getTedTask();
            Date nextRetryTm = taskConfig.retryScheduler.getNextRetryTime(task, task.getRetries() + 1, task.getStartTs());
            task.setIsLastTry(nextRetryTm == null);

            TedProcessor processor = taskConfig.tedProcessorFactory.getProcessor(taskRec1.name);
            result = processor.process(task);

            // check results
            //
            if (result == null) {
                result = TedResult.error("result is null");
            } else if (result.status() == TedStatus.RETRY) {
                if (nextRetryTm == null) {
                    result = TedResult.error("max retries. " + result.message());
                } else {
                    // return as is
                }
            } else if (result.status() == TedStatus.DONE || result.status() == TedStatus.ERROR) {
                // return as is
            } else {
                result = TedResult.error("invalid result status: " + result.status());
            }

        } catch (Throwable e) {
            logger.info("Unhandled exception while calling processor for task '{}': {}", taskRec1.name, e.getMessage());
            taskExceptionLogger.error("Unhandled exception while calling processor for task '" + taskRec1.name + "'", e);
            result = TedResult.error("Catch: " + e.getMessage());
        } finally {
            Thread.currentThread().setName(threadName);
        }
        return result;
    }

}
