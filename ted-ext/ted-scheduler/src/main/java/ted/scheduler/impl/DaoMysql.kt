package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.scheduler.impl.TedSchedulerImpl.Context
import java.sql.Connection

/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 *
 * Dao functions for ted-scheduler (MySql)
 */
internal class DaoMysql(context: Context) : AbstractDao(context) {
    private val logger = LoggerFactory.getLogger(DaoMysql::class.java)
}
