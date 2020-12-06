package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.scheduler.impl.JdbcSelectTed.TedSqlException
import java.sql.Connection
import java.sql.SQLException
import java.sql.Savepoint

/**
 * @author Augustus
 * created on 2019.12.04
 *
 * for TED internal usage only!!!
 */
internal object TxUtils {
    private val logger = LoggerFactory.getLogger(TxUtils::class.java)

    class TxContext(val connection: Connection) {
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
            //logger.trace("start transaction {}", txLogId)
            val txContext = TxContext(connection)

            // call
            val result = function(txContext)

            if (!txContext.rollbacked) {
                logger.trace("[E] before commit transaction {}", txLogId)
                connection.commit()
                logger.trace("[E] after commit transaction {}", txLogId)
            }
            return result

        } catch (e: Throwable) {
            try {
                if (savepoint != null)
                    connection.rollback(savepoint)
                else
                    connection.rollback()
            } catch (rollbackException: Exception) {
                e.addSuppressed(rollbackException)
            }
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


}
