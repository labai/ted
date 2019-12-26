package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.scheduler.impl.TedSchedulerImpl.Context
import java.sql.Connection

/**
 * @author Augustus
 * created on 2019.12.09
 *
 * for TED internal usage only!!!
 *
 * Dao functions for ted-scheduler (HsqlDb)
 */
internal class DaoHsqldb(context: Context) : AbstractDao(context) {
    private val logger = LoggerFactory.getLogger(DaoHsqldb::class.java)

    override fun lockTask(connection: Connection, taskId: Long): Boolean {
        return true // do not use lock
    }

}
