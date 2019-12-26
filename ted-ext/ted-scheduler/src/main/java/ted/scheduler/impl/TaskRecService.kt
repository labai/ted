package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.driver.Ted
import ted.driver.Ted.TedProcessor
import ted.driver.Ted.TedStatus
import ted.driver.TedResult
import ted.driver.sys._TedSchdJdbcSelect.TedSqlException
import ted.scheduler.impl.TedSchedulerImpl.Context
import ted.scheduler.impl.TxUtils.TxContext
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource

/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 */
internal class TaskRecService (private val context: Context) {
    private val logger = LoggerFactory.getLogger(TaskRecService::class.java)


    /* creates task only if does not exists (task + activeStatus).
	   While there are not 100% guarantee, but will try to ensure, that 2 processes will not create same task twice (using this method).
	   If task with ERROR status will be found, then it will be converted to RETRY
	*/
    internal fun createUniqueTask(name: String, data: String?, key1: String?, key2: String?, postponeSec: Int): Long? {

        val resTaskId: Long?
        var createdFakeTaskId : Long? = null

        try {
            var lockTaskId = context.primeTaskIdProvider() ?: context.dao.findFirstTask()

            // no any tasks yet? then create one fake
            // it could be very rare/theoretical situation - only on first system startup. And then two parallel nodes should start at the same time.
            if (lockTaskId == null) {
                context.tedDriver.registerTaskConfig("ted_lock_schd") { TedProcessor { null } }
                createdFakeTaskId = context.tedDriver.createTaskPostponed("ted_lock_schd", "temporary for lock", null, null, 999999999)
                sleepMs(30) // .. just in case
                lockTaskId = context.dao.findFirstTask();
            }

            resTaskId = execWithLockedTask(context.dataSource, lockTaskId!!, 2000) fn@ {
                val taskIds = context.dao.getActiveTasks(name, 2, true)
                if (taskIds.size > 1)
                    throw IllegalStateException("Exists more than one $name active scheduler task (statuses NEW, RETRY, WORK or ERROR) $taskIds, skipping")
                if (taskIds.size == 1) { // exists exactly one task
                    val taskId = taskIds[0]
                    val tedTask = context.tedDriver.getTask(taskId)
                    if (tedTask!!.status == TedStatus.ERROR) {
                        logger.info("Restore scheduler task {} {} from error", name, taskId)
                        context.dao.restoreFromError(taskId, name, postponeSec)
                        return@fn taskId
                    }
                    logger.debug("Exists scheduler task {} with active status (NEW, RETRY or WORK)", name)
                    return@fn taskIds[0]
                }
                logger.debug("No active scheduler tasks {} exists, will create new", name)
                context.tedDriver.createTaskPostponed(name, data, key1, key2, postponeSec)
            }

        } finally {
            if (createdFakeTaskId != null) {
                context.dao.deleteTask(createdFakeTaskId)
            }
        }

        return resTaskId
    }

    // lock task before run.
    // but if lockTaskId is null, then do not lock. it can happen on first run..
    private fun <T> execWithLockedTask(dataSource: DataSource, lockTaskId: Long, timoutMs: Long, function: (TxContext) -> T?): T? {

        val connection: Connection
        try {
            connection = dataSource.connection
        } catch (e: SQLException) {
            logger.error("Failed to get DB connection: " + e.message)
            throw TedSqlException("Cannot get DB connection", e)
        }

        connection.use {
            return TxUtils.txRun(connection) { tx ->
                var succeed = false
                var waited = false
                val startMs = System.currentTimeMillis()
                while (System.currentTimeMillis() - startMs < timoutMs) {
                    succeed = context.dao.lockTask(tx.connection, lockTaskId)
                    if (succeed) break
                    sleepMs(30)
                    waited = true
                }
                if (! succeed) {
                    logger.error("Cannot get advisoryLockPrimeTask for lockTaskId={}, skipping", lockTaskId)
                    return@txRun null
                }
                if (waited) {
                    logger.debug("Waited for lock time={}ms", System.currentTimeMillis() - startMs)
                }
                function(tx)
            }
        }
    }

    private fun sleepMs(ms: Long) {
        try {
            Thread.sleep(ms)
        } catch (e: InterruptedException) {
            throw RuntimeException("Can't sleep", e)
        }

    }

}
