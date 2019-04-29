package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.driver.sys._TedSchdJdbcSelect
import ted.driver.sys._TedSchdJdbcSelect.JetJdbcParamType
import ted.driver.sys._TedSchdJdbcSelect.SqlParam
import ted.driver.sys._TedSchdJdbcSelect.TedSqlException
import ted.driver.sys._TedSchdJdbcSelect.sqlParam
import java.sql.Connection
import java.sql.SQLException
import java.sql.Savepoint
import java.util.Arrays.asList
import java.util.function.Function
import java.util.stream.Collectors
import javax.sql.DataSource
import kotlin.reflect.KClass
import kotlin.streams.toList


/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 *
 * Dao functions for ted-scheduler (PostgreSql)
 */
internal class DaoPostgres(private val dataSource: DataSource, private val thisSystem: String) {
    private val logger = LoggerFactory.getLogger(DaoPostgres::class.java)

    internal class TxContext(val connection: Connection) {
        internal var rollbacked = false
        fun rollback() {
            try {
                connection.rollback()
            } catch (e: SQLException) {
                throw TedSqlException("Cannot rollback", e)
            }

            rollbacked = true
        }
    }

    fun <T> execWithLockedPrimeTaskId(dataSource: DataSource, primeTaskId: Long, function: (TxContext) -> T?): T? {

        val connection: Connection?
        try {
            connection = dataSource.connection
        } catch (e: SQLException) {
            logger.error("Failed to get DB connection: " + e.message)
            throw TedSqlException("Cannot get DB connection", e)
        }

        connection.use {
            return txRun(connection) { tx ->
                if (!advisoryLockPrimeTask(tx.connection, primeTaskId)) {
                    logger.debug("Cannot get advisoryLockPrimeTask for primeTaskId={}, skipping", primeTaskId)
                    return@txRun null
                }
                function(tx)
            }
        }
    }

    // run block in transaction. TxContext will be provided as parameter
    //
    fun <T> txRun(connection: Connection, function: (TxContext) -> T): T {
        var savepoint: Savepoint? = null
        var autocommit: Boolean? = null

        try {
            autocommit = connection.autoCommit
            connection.autoCommit = false
            savepoint = connection.setSavepoint("txRun")
            val txLogId = Integer.toHexString(savepoint!!.hashCode())

            logger.debug("[B] before start transaction {}", txLogId)
            //em.getTransaction().begin();
            logger.trace("[B] after start transaction {}", txLogId)

            val txContext = TxContext(connection)
            val result = function(txContext)

            if (!txContext.rollbacked) {
                logger.trace("[E] before commit transaction {}", txLogId)
                connection.commit()
                logger.debug("[E] after commit transaction {}", txLogId)
            }
            return result

        } catch (e: Throwable) {
            //if (!session.getTransaction().wasCommitted() && !session.getTransaction().wasRolledBack()) {
            try {
                if (savepoint != null)
                    connection.rollback(savepoint)
                else
                    connection.rollback()
            } catch (rollbackException: Exception) {
                e.addSuppressed(rollbackException)
            }

            //}
            throw RuntimeException(e)
        } finally {
            try {
                if (autocommit != null && autocommit != connection.autoCommit) {
                    connection.autoCommit = autocommit
                }
            } catch (e: SQLException) {
                logger.warn("Exception while setting back autoCommit mode", e)
            }
        }
    }

    private class LongVal {
        internal val longVal: Long? = null
    }

    // use advisory lock in Postgres mechanism
    private fun advisoryLockPrimeTask(connection: Connection, primeTaskId: Long): Boolean {
        logger.debug("advisory lock {}", primeTaskId)
        val sql = "select case when pg_try_advisory_xact_lock(1977110801, $primeTaskId) then 1 else 0 end as longVal"
        val res: List<LongVal>
        try {
            res = _TedSchdJdbcSelect.selectData(connection, sql, LongVal::class.java, emptyList<SqlParam>())
        } catch (e: SQLException) {
            return false
        }
        return ! (res.isEmpty() || res[0].longVal == 0L)
    }


    // returned count: 0 - not exists, 1 - exists 1, 2 - exists more than 1
    // includingError - do include with status ERROR?
    // 'SLEEP'?
    fun get2ActiveTasks(taskName: String, includingError: Boolean): List<Long> {
        val sqlLogId = "chk_uniq_task"
        val sql = ("select taskid as longVal from tedtask where system = '$thisSystem' and name = ?"
                + " and status in ('NEW', 'RETRY', 'WORK'" + (if (includingError) ",'ERROR'" else "") + ")"
                + " limit 2")
        val results = selectData(sqlLogId, sql, LongVal::class, listOf(
                sqlParam(taskName, JetJdbcParamType.STRING)
        ))
        return results.stream().map<Long> { longVal -> longVal.longVal }.toList()
    }

    fun restoreFromError(taskId: Long, taskName: String, postponeSec: Int) {
        val sqlLogId = "restore_from_error"
        val sql = ("update tedtask set status = 'RETRY', retries = retries + 1, "
                + " nextts = now() + interval '$postponeSec seconds' "
                + " where system = '$thisSystem' and taskid = ? and name = ?"
                + " and status = 'ERROR'"
                + " returning tedtask.taskid")

        selectData(sqlLogId, sql, LongVal::class, listOf(
                sqlParam(taskId, JetJdbcParamType.LONG),
                sqlParam(taskName, JetJdbcParamType.STRING)
        ))
    }

    fun checkForErrorStatus(taskIds: Collection<Long>): List<Long> {
        val sqlLogId = "check_for_errors"
        if (taskIds.isEmpty())
            return emptyList()

        val inIds = taskIds.stream().map { it.toString() }.collect(Collectors.joining(","))
        val sql = ("select taskid as longVal from tedtask"
                + " where system = '$thisSystem'"
                + " and status = 'ERROR'"
                + " and taskid in ($inIds)")

        val results = selectData(sqlLogId, sql, LongVal::class, emptyList())
        return results.stream().map<Long> { longVal -> longVal.longVal }.toList()
    }


    fun <T : Any> selectData(sqlLogId: String, sql: String, clazz: KClass<T>, params: List<SqlParam>): List<T> {
        val startTm = System.currentTimeMillis()
        val list = _TedSchdJdbcSelect.selectData(dataSource, sql, clazz.java, params)
        val durationMs = System.currentTimeMillis() - startTm
        if (durationMs >= 50)
            logger.info("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size)
        else
            logger.debug("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size)
        return list
    }



}
