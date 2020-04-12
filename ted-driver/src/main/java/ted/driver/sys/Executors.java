package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.TedDao.SetTaskStatus;
import ted.driver.sys.TedDriverImpl.TedContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
		private final List<TaskRec> tasks;
		public TedRunnable(TaskRec task) {
			this.task = task;
			this.tasks = null;
		}
		public TedRunnable(List<TaskRec> tasks) {
			this.task = null;
			this.tasks = new ArrayList<>(tasks);
		}
		public List<TaskRec> getTasks() {
			if (tasks != null)
				return tasks;
			return Collections.singletonList(task);
		}
		public int getTaskCount() {
			return (tasks != null ? tasks.size() : 1);
		}
	}

	/**
	 * ThreadPoolExecutor for channel workers.
	 *
	 * will know working runnables
	 */
	class ChannelThreadPoolExecutor extends ThreadPoolExecutor {
		private final WeakHashMap<Thread, Runnable> threads;
		private final String channel;

		ChannelThreadPoolExecutor(String channel, int workerCount, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
			super(workerCount, workerCount, 0, TimeUnit.SECONDS, workQueue, threadFactory);

			this.channel = channel;
			this.threads = new WeakHashMap<>(workerCount);

			setRejectedExecutionHandler(new TaskRejectedExecutionHandler());
		}

		protected void beforeExecute(Thread t, Runnable r) {
			threads.put(t, r);
			// logger.debug("beforeExecute" + t + " - " + r + " - ");
			super.beforeExecute(t, r);
		}

		protected void afterExecute(Runnable r, Throwable t) {
			super.afterExecute(r, t);
			//logger.debug("afterExecute" + Thread.currentThread() + " - " + r);
			threads.remove(Thread.currentThread());
		}

		private List<TaskRec> getWorkingTasks() {
			List<TaskRec> tasks = new ArrayList<>();
			for (Entry<Thread, Runnable> entry : threads.entrySet()) {
				try {
					if (entry.getValue() instanceof TedRunnable)
						tasks.addAll(((TedRunnable)entry.getValue()).getTasks());
				} catch (Throwable e) {
					logger.info("Cannot get working thread task", e);
				}
			}
			return tasks;
		}

		// TODO
		// if task already was finished just before. If we will update to NEW, it will retry. It is not very bad, as tasks should be programmed to be able to retry any count
		// if there will be 1000nds of tasks (e.g. on PackProcessing).
		// if shutdown will continue more than 40s, same task may start in parallel in another node.
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
	}


	/**
	 * will bring back TedTask to status NEW
	 */
	private class TaskRejectedExecutionHandler implements RejectedExecutionHandler {
		@Override
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			if (r instanceof TedRunnable) {
				TedRunnable tr = (TedRunnable) r;
				List<TaskRec> tasks = tr.getTasks();
				List<SetTaskStatus> statuses = new ArrayList<>();
				for (TaskRec task : tasks) {
					logger.info("Task {} was rejected by executor, returning to status NEW", task);
					statuses.add(new SetTaskStatus(task.taskId, TedStatus.NEW, Model.REJECTED_MSG, new Date(System.currentTimeMillis() + 5000)));
				}
				context.tedDao.setStatuses(statuses);
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
	ThreadPoolExecutor createChannelExecutor(String channel, final String threadPrefix, final int workerCount, int queueSize) {
		ThreadFactory threadFactory = new ThreadFactory() {
			private int counter = 0;
			@Override
			public Thread newThread(Runnable runnable) {
				return new Thread(runnable, threadPrefix + "-" + ++counter);
			}
		};
		ThreadPoolExecutor executor = new ChannelThreadPoolExecutor(channel, workerCount,
				new LinkedBlockingQueue<Runnable>(queueSize), threadFactory);
		return executor;
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
		ThreadPoolExecutor executor = new ThreadPoolExecutor(workerCount, workerCount,
				0, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(queueSize), threadFactory);
		return executor;
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
		ScheduledExecutorService executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(threadFactory);
		return executor;
	}

}
