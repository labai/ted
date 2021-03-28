package ted.scheduler.impl

import java.sql.Connection

/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 */
internal interface ISchedulerDao {
    fun lockTask(connection: Connection, taskId: Long): Boolean
    fun findFirstTask(): Long?
    fun getActiveTasks(taskName: String, limit: Int, includingError: Boolean): List<Long>
    fun restoreFromError(taskId: Long, taskName: String, postponeSec: Int)
    fun checkForErrorStatus(taskIds: Collection<Long>): List<Long>
    fun deleteTask(taskId: Long)
}
