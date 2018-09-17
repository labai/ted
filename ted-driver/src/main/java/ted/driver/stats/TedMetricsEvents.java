package ted.driver.stats;

import ted.driver.Ted.TedStatus;

/**
 * @author Augustus
 *         created on 2018-09-16
 *
 * metrics functions. will be called from ted-driver. can be used to measure ted work parameters.
 *
 * Experimental. Will be changed in next releases
 */
public interface TedMetricsEvents {
	void dbCall(String logId, int resultCount, int durationMs);

	void loadTask(long taskId, String taskName, String channel);
	void startTask(long taskId, String taskName, String channel);
	void finishTask(long taskId, String taskName, String channel, TedStatus status, int durationMs);
}
