package ted.driver.sys;

import ted.driver.sys.Model.TaskRec;

import java.util.Date;
import java.util.List;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 */
interface TedDaoExt {

//	Map<TedStatus, Integer> getBatchStatusStats(long batchId);

	boolean becomePrime(Long primeTaskId, String instanceId);

	Long findPrimeTaskId();

	Long createEvent(String taskName, String queueId, String data, String key2);

	TaskRec eventQueueMakeFirst(String queueId);

	List<TaskRec> eventQueueGetTail(String queueId);

	TaskRec eventQueueReserveTask(long taskId);

	List<TaskRec> getLastNotifications(Date fromTs);

	void cleanupNotifications(Date tillTs);

	void runInTx(Runnable runnable);

	boolean maintenanceRebuildIndex();

}
