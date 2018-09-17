package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.stats.TedMetricsEvents;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author Augustus
 *         created on 2018.09.16
 *
 * for TED internal usage only!!!
 *
 */
class Stats {
	private static final Logger logger = LoggerFactory.getLogger(Stats.class);

	public final TedMetricsEvents metrics = createTedStatsProxy(TedMetricsEvents.class);

	private final ExecutorService statsEventExecutor;

	private TedMetricsEvents registryExt = null;
	private TedMetricsEvents registryInternal = new TedMetricsEventsInternal();


	public Stats(ExecutorService statsEventExecutor) {
		this.statsEventExecutor = statsEventExecutor;
	}

	public void setMetricsRegistry(TedMetricsEvents registry) {
		this.registryExt = registry;
	}

	@SuppressWarnings("unchecked")
	private <T> T createTedStatsProxy(Class<T> iface) {
		return (T) Proxy.newProxyInstance(
				iface.getClassLoader(),
				new Class<?>[] { iface },
				new TedMetricsInvocationHandler());
	}

	private class TedMetricsInvocationHandler<T> implements InvocationHandler {
		//private final Set<Method> methodSkipList = new HashSet<Method>();
		@Override
		public Object invoke(Object proxy, final Method method, final Object[] args) {
			try {
				// internal
				Object result = method.invoke(registryInternal, args);

				// external
				if (registryExt == null)
					return null;
				//if (methodSkipList.contains(method))
				//	return null;
				statsEventExecutor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							long startMs = System.currentTimeMillis();
							Object result = method.invoke(registryExt, args);
							if (System.currentTimeMillis() - startMs > 10) // 10ms just for registering
								logger.debug("ted stats long execution, method={}, time={}ms", method.getName(), System.currentTimeMillis() - startMs);
						} catch (Throwable e) {
							//methodSkipList.add(method); // do we need this?
							logger.warn("Exception while registering metrics (method " + method.getName() + "). Next method calls will be skipped", e);
						}
					}
				});
			} catch (RejectedExecutionException e) {
				logger.warn("Exception while registering metrics (method " + method.getName() + "). Queue is full? " + e.getMessage());
			} catch (Exception e) {
				logger.warn("Exception while registering metrics (method " + method.getName() + ")", e);
			}
			return null; // should be voids
		}
	}

	private class TedMetricsEventsInternal implements TedMetricsEvents {
		@Override
		public void dbCall(String logId, int resultCount, int durationMs) {
			if (durationMs >= 50)
				logger.info("After [{}] time={}ms items={}", logId, durationMs, resultCount);
			else
				logger.debug("After [{}] time={}ms items={}", logId, durationMs, resultCount);
		}
		@Override public void loadTask(long taskId, String taskName, String channel) {}
		@Override public void startTask(long taskId, String taskName, String channel) {}
		@Override public void finishTask(long taskId, String taskName, String channel, TedStatus status, int durationMs) { }
	}


}

