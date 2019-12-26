package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.driver.sys._TedSchdJdbcSelect
import ted.driver.sys._TedSchdJdbcSelect.JetJdbcParamType.LONG
import ted.driver.sys._TedSchdJdbcSelect.JetJdbcParamType.STRING
import ted.driver.sys._TedSchdJdbcSelect.SqlParam
import ted.scheduler.impl.TedSchedulerImpl.Context
import java.sql.Connection
import kotlin.reflect.KClass

/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 *
 * Dao functions for ted-scheduler (common for all db)
 */
internal abstract class AbstractDao (private val context: Context) : ISchedulerDao {
    private val logger = LoggerFactory.getLogger(AbstractDao::class.java)

    private val dataSource = context.dataSource
    private val thisSystem = context.thisSystem
    private val dbType = context.dbType


    override fun lockTask(connection: Connection, taskId: Long): Boolean {
        val sql = ("select taskid as longVal"
                + " from tedtask"
                + " where taskid = ?"
                + dbType.sql().forUpdateSkipLocked()
                )
        val list = selectData("lock_task", sql, LongVal::class, listOf(
                    _TedSchdJdbcSelect.sqlParam(taskId, LONG)
                ))
        return list.firstOrNull()?.longVal == taskId
    }

    // includingError - do include with status ERROR?
    // 'SLEEP'?
    override fun getActiveTasks(taskName: String, limit: Int, includingError: Boolean): List<Long> {
        val sqlLogId = "chk_uniq_task"
        val sql = ("select taskid as longVal from tedtask where system = '$thisSystem' and name = ?"
                + " and status in ('NEW', 'RETRY', 'WORK'" + (if (includingError) ",'ERROR'" else "") + ")"
                + dbType.sql().rownum(limit)
                )
        val results = selectData(sqlLogId, sql, LongVal::class, listOf(
                _TedSchdJdbcSelect.sqlParam(taskName, STRING)
        ))
        return results.map { it.longVal!! }
    }

    override fun restoreFromError(taskId: Long, taskName: String, postponeSec: Int) {
        val sqlLogId = "restore_from_error"
        val sql = ("update tedtask set status = 'RETRY', retries = retries + 1, "
                + " nextts = now() + interval '$postponeSec seconds' "
                + " where system = '$thisSystem' and taskid = ? and name = ?"
                + " and status = 'ERROR'"
                // + " returning tedtask.taskid"
                )

        executeUpdate(sqlLogId, sql, listOf(
                _TedSchdJdbcSelect.sqlParam(taskId, LONG),
                _TedSchdJdbcSelect.sqlParam(taskName, STRING)
        ))
    }

    override fun checkForErrorStatus(taskIds: Collection<Long>): List<Long> {
        val sqlLogId = "check_for_errors"
        if (taskIds.isEmpty())
            return emptyList()

        val inIds = taskIds.map { it.toString() }.joinToString(",")
        val sql = ("select taskid as longVal from tedtask"
                + " where system = '$thisSystem'"
                + " and status = 'ERROR'"
                + " and taskid in ($inIds)")

        val results = selectData(sqlLogId, sql, LongVal::class, emptyList())
        return results.map { it.longVal!! }
    }

    override fun findFirstTask(): Long? {
        val sql = "select min(taskId) as longVal from tedtask where system = '$thisSystem'"

        return selectData("min_taskid", sql, LongVal::class, emptyList())
                .firstOrNull()?.longVal
    }


    override fun deleteTask(taskId: Long) {
        val sql = "delete from tedtask where taskId = $taskId"
        executeUpdate("del_task", sql, emptyList())
    }


    //
    //
    //

    internal fun <T : Any> selectData(sqlLogId: String, sql: String, clazz: KClass<T>, params: List<SqlParam>): List<T> {
        val startTm = System.currentTimeMillis()
        val list = _TedSchdJdbcSelect.selectData(dataSource, sql, clazz.java, params)
        val durationMs = System.currentTimeMillis() - startTm
        if (durationMs >= 50)
            logger.info("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size)
        else
            logger.debug("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size)
        return list
    }

    internal fun executeUpdate(sqlLogId: String, sql: String, params: List<SqlParam>): Int {
        val startTm = System.currentTimeMillis()
        val res = _TedSchdJdbcSelect.executeUpdate(dataSource, sql, params)
        val durationMs = System.currentTimeMillis() - startTm
        if (durationMs >= 50)
            logger.info("After [{}] time={}ms items={}", sqlLogId, durationMs, res)
        else
            logger.debug("After [{}] time={}ms items={}", sqlLogId, durationMs, res)
        return res
    }

    //
    //
    //

    private class LongVal {
        internal val longVal: Long? = null
    }

}

