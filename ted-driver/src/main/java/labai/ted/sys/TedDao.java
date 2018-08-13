package labai.ted.sys;

import labai.ted.Ted.TedStatus;
import labai.ted.sys.Model.TaskParam;
import labai.ted.sys.Model.TaskRec;
import labai.ted.sys.PrimeInstance.CheckPrimeParams;
import labai.ted.sys.QuickCheck.CheckResult;
import labai.ted.sys.TedDaoAbstract.DbType;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 */
interface TedDao {

	DbType getDbType();

	Long createTask(String name, String channel, String data, String key1, String key2, Long batchId);

	Long createTaskPostponed(String name, String channel, String data, String key1, String key2, int postponeSec);

	Long createTaskWithWorkStatus(String name, String channel, String data, String key1, String key2);

	List<Long> createTasksBulk(List<TaskParam> taskParams);

	void processMaintenanceFrequent();

	void processMaintenanceRare(int deleteAfterDays);

	List<TaskRec> getWorkingTooLong();

	void setTaskPlannedWorkTimeout(long taskId, Date timeoutTime);

	// quick check, is there any task
	List<String> getWaitChannels();

	List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes);

	void setStatus(long taskId, TedStatus status, String msg);

	void setStatusPostponed(long taskId, TedStatus status, String msg, Date nextRetryTs);

	TaskRec getTask(long taskId);

	boolean checkIsBatchFinished(long batchId);

	void cleanupBatchTask(Long taskId, String msg, String chanel);

	Map<TedStatus, Integer> getBatchStatusStats(long batchId);

	List<CheckResult> quickCheck(CheckPrimeParams checkPrimeParams);

	boolean becomePrime(Long primeTaskId, String instanceId);

	Long findPrimeTaskId();

	Long createEvent(String taskName, String queueId, String data, String key2);

	TaskRec eventQueueMakeFirst(String queueId);

	List<TaskRec> eventQueueGetTail(String queueId);

	List<TaskRec> getLastNotifications(Date fromTs);

	void cleanupNotifications(Date tillTs);

}
