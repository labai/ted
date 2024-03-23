package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.TedDao.SetTaskStatus;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static ted.driver.sys.MiscUtils.asList;

/**
 * @author Augustus
 *         created on 2018.10.06
 *
 * for TED internal usage only!!!
 *
 * various functions and classes related to executor, runnables
 *
 */
class Executors {
    private static final Logger logger = LoggerFactory.getLogger(Executors.class);

    private final TedContext context;

    /**
     * runnable for TedTask(s) execution
     */
    abstract static class TedRunnable implements Runnable {
        private final TaskRec task;
        protected TedRunnable(TaskRec task) {
            this.task = task;
        }
        public TaskRec getTask() { return task; }
        public int getTaskCount() { return 1; }
    }

    static class MeasuredRunnable {
        final Runnable runnable;
        final Long startNs;
        public MeasuredRunnable(Runnable r, Long startNs) {
            this.runnable = r;
            this.startNs = startNs;
        }
    }

    /**
     * ThreadPoolExecutor for channel workers.
     *
     * will know working runnables
     */
    class ChannelThreadPoolExecutor extends ThreadPoolExecutor {
        final Map<Thread, MeasuredRunnable> threads;
        private final String channel;

        ChannelThreadPoolExecutor(String channel, int workerCount, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            super(workerCount, workerCount, 0, TimeUnit.SECONDS, workQueue, threadFactory);

            this.channel = channel;
            this.threads = new HashMap<>(workerCount);

            setRejectedExecutionHandler(new TaskRejectedExecutionHandler());
        }

        @Override
        protected void beforeExecute(Thread t, Runnable r) {
            threads.put(t, new MeasuredRunnable(r, System.nanoTime()));
            super.beforeExecute(t, r);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            threads.remove(Thread.currentThread());
        }

        private List<TaskRec> getWorkingTasks() {
            List<TaskRec> tasks = new ArrayList<>();
            for (Entry<Thread, MeasuredRunnable> entry : threads.entrySet()) {
                try {
                    if (entry.getValue().runnable instanceof TedRunnable)
                        tasks.add(((TedRunnable)entry.getValue().runnable).getTask());
                } catch (Exception e) {
                    logger.info("Cannot get working thread task", e);
                }
            }
            return tasks;
        }

        private Entry<Thread, MeasuredRunnable> findRunningTask(long taskId) {
            for (Entry<Thread, MeasuredRunnable> entry : threads.entrySet()) {
                if (!(entry.getValue().runnable instanceof TedRunnable))
                    continue;
                TedRunnable tedRunnable = (TedRunnable) entry.getValue().runnable;
                if (tedRunnable.getTask().taskId == taskId)
                    return entry;
            }
            return null;
        }

        boolean findAndInterruptTask(long taskId) {
            int totalTimeSec = 5;

            long startTs = System.currentTimeMillis();
            long tillTs = startTs + totalTimeSec * 1000;

            Entry<Thread, MeasuredRunnable> entry = findRunningTask(taskId);
            if (entry == null)
                return false;

            logger.debug("Start to stop (interrupt) taskId={}", taskId);
            Thread thread = entry.getKey();

            if (!isTaskRunning(thread, taskId))
                return true;
            boolean res = interruptThread(thread, taskId, tillTs);
            if (res) {
                logger.info("Successful stop (interrupt) of thread of taskId={}", taskId);
                return true;
            }
            return false;
        }

        boolean findAndKillTask(long taskId) {
            Entry<Thread, MeasuredRunnable> entry = findRunningTask(taskId);
            if (entry == null)
                return false;

            Thread thread = entry.getKey();
            if (!isTaskRunning(thread, taskId))
                return true;
            logger.info("Hard stop of thread for taskId={}", taskId);
            try {
                //noinspection deprecation
                thread.stop();
            } catch (Throwable e) {
                logger.info("Failed to Thread.stop() taskId=" + taskId, e);
                return false;
            }
            return true;
        }

        private boolean isTaskRunning(Thread thread, long taskId) {
            MeasuredRunnable mr = threads.get(thread);
            return mr != null
                && (mr.runnable instanceof TedRunnable)
                && ((TedRunnable) mr.runnable).getTask().taskId == taskId;
        }

        private boolean interruptThread(Thread thread, long taskId, long tillTs) {
            thread.interrupt();
            // wait
            long now = System.currentTimeMillis();
            while (now < tillTs) {
                try {
                    Thread.sleep(10);
                    if (!thread.isAlive() || !isTaskRunning(thread, taskId))
                        return true;
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    logger.info("Thread interrupt wait exception taskId={}", taskId);
                    Thread.currentThread().interrupt();
                    break;
                }
                now = System.currentTimeMillis();
            }
            boolean isRunning = isTaskRunning(thread, taskId);
            logger.debug("Thread interrupt finished alive={} isRunning={}", thread.isAlive(), isRunning);
            return !thread.isAlive() || !isRunning;
        }

        // return tasks and their running time in milliseconds
        public Map<TaskRec, Long> getLongRunningTasks() {
            long thresholdNs = 300_000_000L; // 5 min
            Map<TaskRec, Long> tasks = new HashMap<>();
            Long nowNs = System.nanoTime();
            for (Entry<Thread, MeasuredRunnable> entry : threads.entrySet()) {
                try {
                    MeasuredRunnable mrun = entry.getValue();
                    if (nowNs - mrun.startNs < thresholdNs)
                        continue;
                    if (!(mrun.runnable instanceof TedRunnable))
                        continue;
                    tasks.put(((TedRunnable) mrun.runnable).getTask(), (nowNs - mrun.startNs) / 1_000_000L);
                } catch (Exception e) {
                    logger.debug("Cannot get working thread task", e); // parallel issues?
                }
            }
            return tasks;
        }

        // if task already was finished just before. If we update to NEW, it will retry. It is not very bad, as tasks should be programmed to be able to retry any count
        public void handleWorkingTasksOnShutdown() {
            int postponeSec = 40;
            List<TaskRec> tasks = getWorkingTasks();
            List<SetTaskStatus> statuses = new ArrayList<>();
            for (TaskRec task : tasks) {
                logger.info("return back working task {} (taskId={}) to status RETRY", task.name, task.taskId);
                statuses.add(new SetTaskStatus(task.taskId, TedStatus.RETRY, Model.TIMEOUT_MSG + " (stopped on shutdown)", new Date(System.currentTimeMillis() + postponeSec * 1000)));
            }
            context.tedDao.setStatuses(statuses);
        }

        // clean map if there are dead threads
        public void cleanup() {
            Set<Thread> threadSet = threads.keySet();
            for (Thread th : threadSet) {
                if (th == null || !th.isAlive()) {
                    threads.remove(th);
                }
            }
        }
    }

    /**
     * will bring back TedTask to status NEW
     */
    private class TaskRejectedExecutionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof TedRunnable) {
                TedRunnable tr = (TedRunnable) r;
                TaskRec task = tr.getTask();
                logger.info("Task {} was rejected by executor, returning to status NEW", task);
                SetTaskStatus status = new SetTaskStatus(task.taskId, TedStatus.NEW, Model.REJECTED_MSG, new Date(System.currentTimeMillis() + 5000));
                context.tedDao.setStatuses(asList(status));
            } else {
                throw new RejectedExecutionException("ThreadPoolExecutor rejected runnable");
            }

        }
    }

    Executors(TedContext context) {
        this.context = context;
    }

    /**
     * create ThreadPoolExecutor for channel
     */
    ChannelThreadPoolExecutor createChannelExecutor(String channel, final String threadPrefix, final int workerCount, int queueSize) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private int counter = 0;
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, threadPrefix + "-" + ++counter);
            }
        };
        return new ChannelThreadPoolExecutor(channel, workerCount, new LinkedBlockingQueue<>(queueSize), threadFactory);
    }

    /**
     * create general purpose ThreadPoolExecutor
     */
    ThreadPoolExecutor createExecutor(final String threadPrefix, final int workerCount, int queueSize) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private int counter = 0;
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, threadPrefix + "-" + ++counter);
            }
        };
        return new ThreadPoolExecutor(workerCount, workerCount,
            0, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(queueSize), threadFactory);
    }

    /**
     * create scheduler ThreadPoolExecutor
     */
    ScheduledExecutorService createSchedulerExecutor(final String prefix) {
        ThreadFactory threadFactory = new ThreadFactory() {
            private int counter = 0;
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, prefix + ++counter);
            }
        };
        return java.util.concurrent.Executors.newSingleThreadScheduledExecutor(threadFactory);
    }
}
