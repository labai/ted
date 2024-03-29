package ted.driver.sys;

import ted.driver.Ted.TedStatus;
import ted.driver.sys.Model.TaskParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.PrimeInstance.CheckPrimeParams;
import ted.driver.sys.QuickCheck.CheckResult;
import ted.driver.sys.QuickCheck.GetWaitChannelsResult;
import ted.driver.sys.QuickCheck.Tick;
import ted.driver.sys.SqlUtils.DbType;

import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static ted.driver.sys.MiscUtils.asList;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 */
interface TedDao {

    DbType getDbType();

    Long createTask(String name, String channel, String data, String key1, String key2, Long batchId, Connection conn);

    Long createTaskPostponed(String name, String channel, String data, String key1, String key2, int postponeSec, Connection conn);

    Long createTaskWithWorkStatus(String name, String channel, String data, String key1, String key2, Connection conn);

    List<Long> createTasksBulk(List<TaskParam> taskParams, Connection connection);

    void processMaintenanceFrequent();

    void processMaintenanceRare();

    void maintenanceDeleteTasks(int deleteAfterDays, String tableName);

    List<TaskRec> getWorkingTooLong();

    void setTaskPlannedWorkTimeout(long taskId, Date timeoutTime);

    // quick check, is there any task
    List<GetWaitChannelsResult> getWaitChannels(Tick tick);

    List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes, Tick tick);

    void setStatuses(List<SetTaskStatus> statuses, Connection conn);

    TaskRec getTask(long taskId);

    boolean checkIsBatchFinished(long batchId);

    void cleanupBatchTask(Long taskId, String msg, String channel);

    List<CheckResult> quickCheck(CheckPrimeParams checkPrimeParams, Tick tick);

    class SetTaskStatus {
        final long taskId;
        final TedStatus status;
        final String msg;
        final Date nextRetryTs;
        final Date startTs;
        final Date updateTs = new Date();
        public SetTaskStatus(long taskId, TedStatus status, String msg, Date nextRetryTs, Date startTs) {
            this.taskId = taskId;
            this.status = status;
            this.msg = msg;
            this.nextRetryTs = nextRetryTs;
            this.startTs = startTs;
        }
        public SetTaskStatus(long taskId, TedStatus status, String msg, Date nextRetryTs) {
            this(taskId, status, msg, nextRetryTs, null);
        }
        public SetTaskStatus(long taskId, TedStatus status, String msg) {
            this(taskId, status, msg, null, null);
        }

        @Override public String toString() { return "{" + taskId + " " + status + "}"; }
    }

    default void setStatus(long taskId, TedStatus status, String msg) {
        setStatuses(asList(new SetTaskStatus(taskId, status, msg)));
    }

    default void setStatusPostponed(long taskId, TedStatus status, String msg, Date nextTs) {
        setStatuses(asList(new SetTaskStatus(taskId, status, msg, nextTs)));
    }

    default void setStatuses(List<SetTaskStatus> statuses) {
        setStatuses(statuses, null);
    }
}
