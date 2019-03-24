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


    fun <T> execWithLockedPrimeTaskId(dataSource: DataSource, primeTaskId: Long?, function: Function<TxContext, T?>): T? {
        if (primeTaskId == null) throw IllegalStateException("primeTaskId is null")

        val connection: Connection?
        try {
            connection = dataSource.connection
        } catch (e: SQLException) {
            logger.error("Failed to get DB connection: " + e.message)
            throw TedSqlException("Cannot get DB connection", e)
        }

        try {
            return txRun(connection, Function() fn@{ tx ->
                if (!advisoryLockPrimeTask(tx.connection, primeTaskId)) {
                    logger.debug("Cannot get advisoryLockPrimeTask for primeTaskId={}, skipping", primeTaskId)
                    return@fn null
                }
                function.apply(tx)
            })

        } finally {
            try { connection?.close() } catch (e: Exception) { logger.error("Cannot close connection", e) }
        }
    }

    fun <T> txRun(connection: Connection?, function: Function<TxContext, T>): T {
        var savepoint: Savepoint? = null
        var autocommit: Boolean? = null

        try {
            autocommit = connection!!.autoCommit
            connection.autoCommit = false
            savepoint = connection.setSavepoint("txRun")
            val txLogId = Integer.toHexString(savepoint!!.hashCode())

            logger.debug("[B] before start transaction {}", txLogId)
            //em.getTransaction().begin();
            logger.trace("[B] after start transaction {}", txLogId)

            val txContext = TxContext(connection)
            val result = function.apply(txContext)

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
                    connection!!.rollback(savepoint)
                else
                    connection!!.rollback()
            } catch (rollbackException: Exception) {
                e.addSuppressed(rollbackException)
            }

            //}
            throw RuntimeException(e)
        } finally {
            try {
                if (autocommit != null && autocommit != connection!!.autoCommit) {
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

        return if (res.isEmpty() || res[0].longVal == 0L) false else true
    }


    // returned count: 0 - not exists, 1 - exists 1, 2 - exists more than 1
    // includingError - do include with status ERROR?
    // todo SLEEP?
    fun get2ActiveTasks(taskName: String, includingError: Boolean): List<Long> {
        val sqlLogId = "chk_uniq_task"
        val sql = ("select taskid as longVal from tedtask where system = '$thisSystem' and name = ?"
                + " and status in ('NEW', 'RETRY', 'WORK'" + (if (includingError) ",'ERROR'" else "") + ")"
                + " limit 2")
        //sql = sql.replace("\$sys", thisSystem)
        val results = selectData<LongVal>(sqlLogId, sql, LongVal::class.java, asList(
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
//        sql = sql.replace("\$sys", thisSystem)
//        sql = sql.replace("\$postponeSec", postponeSec.toString())

        selectData<LongVal>(sqlLogId, sql, LongVal::class.java, asList(
                sqlParam(taskId, JetJdbcParamType.LONG),
                sqlParam(taskName, JetJdbcParamType.STRING)
        ))
    }

    fun checkForErrorStatus(taskIds: Collection<Long>): List<Long> {
        val sqlLogId = "check_for_errors"
        if (taskIds.isEmpty())
            return emptyList<Long>()
        val inIds = taskIds.stream().map { it.toString() }.collect(Collectors.joining(","))
        var sql = ("select taskid as longVal from tedtask"
                + " where system = '\$sys'"
                + " and status = 'ERROR'"
                + " and taskid in (" + inIds + ")")
        sql = sql.replace("\$sys", thisSystem)

        val results = selectData(sqlLogId, sql, LongVal::class.java, emptyList())
        return results.stream().map<Long> { longVal -> longVal.longVal }.toList()
    }


    fun <T> selectData(sqlLogId: String, sql: String, clazz: Class<T>, params: List<SqlParam>): List<T> {
        val startTm = System.currentTimeMillis()
        val list = _TedSchdJdbcSelect.selectData(dataSource, sql, clazz, params)
        val durationMs = System.currentTimeMillis() - startTm
        if (durationMs >= 50)
            logger.info("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size)
        else
            logger.debug("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size)
        return list
    }

}
