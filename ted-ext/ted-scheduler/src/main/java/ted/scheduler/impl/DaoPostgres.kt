package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.scheduler.impl.JdbcSelectTed.SqlParam
import ted.scheduler.impl.TedSchedulerImpl.Context
import java.sql.Connection
import java.sql.SQLException

/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 *
 * Dao functions for ted-scheduler (PostgreSql)
 */
internal class DaoPostgres(context: Context) : AbstractDao(context) {
    private val logger = LoggerFactory.getLogger(DaoPostgres::class.java)

    private class LongVal {
        internal val longVal: Long? = null
    }

    override fun lockTask(connection: Connection, taskId: Long): Boolean {
        return advisoryLockTask(connection, taskId)
    }

    // use advisory lock in Postgres mechanism
    private fun advisoryLockTask(connection: Connection, lockTaskId: Long): Boolean {
        logger.debug("advisory lock {}", lockTaskId)
        val sql = "select case when pg_try_advisory_xact_lock(1977110802, $lockTaskId) then 1 else 0 end as longVal"
        val res: List<LongVal>
        try {
            res = SchdJdbcSelect.selectData(connection, sql, LongVal::class.java, emptyList<SqlParam>())
        } catch (e: SQLException) {
            logger.info("advisory lock {} SQLException: {} {}", lockTaskId, e.errorCode, e.message)
            return false
        }
        return ! (res.isEmpty() || res[0].longVal == 0L)
    }


}
