package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.scheduler.impl.TedSchedulerImpl.Context

/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 *
 * Dao functions for ted-scheduler (Oracle)
 */
internal class DaoOracle(context: Context) : AbstractDao(context) {
    private val logger = LoggerFactory.getLogger(DaoOracle::class.java)

}
