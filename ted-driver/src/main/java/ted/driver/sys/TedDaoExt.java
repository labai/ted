package ted.driver.sys;

import ted.driver.sys.Model.TaskRec;

import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 */
interface TedDaoExt {

//	Map<TedStatus, Integer> getBatchStatusStats(long batchId);

    boolean becomePrime(Long primeTaskId, String instanceId, Integer postponeSec);

    Long findPrimeTaskId();

    Long createEvent(String taskName, String queueId, String data, String key2);

    TaskRec eventQueueMakeFirst(String queueId, int postponeSec);

    List<TaskRec> eventQueueGetTail(String queueId);

    TaskRec eventQueueReserveTask(long taskId);

    List<TaskRec> getLastNotifications(Date fromTs);

    void cleanupNotifications(Date tillTs);

    <T> void runInTx(Function<Connection, T> function);

    boolean maintenanceRebuildIndex();

    void maintenanceMoveDoneTasks(String archiveTableName, int mainDeleteAfterDays);

}
