package ted.driver.sys;

import ted.driver.sys.Model.TaskRec;

import java.util.Date;
import java.util.List;

/**
 * @author Augustus
 *         created on 2018.09.07
 *
 * for TED internal usage only!!!
 *
 */
class TedDaoExtNA implements TedDaoExt {
    private final String errorMsg;

    TedDaoExtNA(String forDbType) {
        this.errorMsg = "Not supported for " + forDbType;
    }

    @Override
    public boolean becomePrime(Long primeTaskId, String instanceId, Integer postponeSec) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public Long findPrimeTaskId() {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public Long createEvent(String taskName, String queueId, String data, String key2) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public TaskRec eventQueueMakeFirst(String queueId) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public List<TaskRec> eventQueueGetTail(String queueId) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public TaskRec eventQueueReserveTask(long taskId) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public List<TaskRec> getLastNotifications(Date fromTs) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public void cleanupNotifications(Date tillTs) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public void runInTx(Runnable runnable) {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public boolean maintenanceRebuildIndex() {
        throw new IllegalStateException(errorMsg);
    }

    @Override
    public void maintenanceMoveDoneTasks(String histTableName, int deleteHistoryDays) {
        throw new IllegalStateException(errorMsg);
    }
}
