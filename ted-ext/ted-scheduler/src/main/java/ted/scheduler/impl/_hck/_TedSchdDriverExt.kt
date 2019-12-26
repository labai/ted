package ted.driver.sys


import ted.driver.TedDriver
import ted.driver._TedSchdHck
import ted.driver.sys.SqlUtils.DbType
import javax.sql.DataSource

/**
 * @author Augustus
 * created on 2018.08.17
 *
 * use some of internal functions.
 *
 * For ted-scheduler only!
 */
class _TedSchdDriverExt(private val tedDriver: TedDriver) {
    private val tedDriverImpl: TedDriverImpl
    val isPrimeEnabled: Boolean
        get() = tedDriverImpl.prime()!!.isEnabled()

    init {
        this.tedDriverImpl = _TedSchdHck.getTedDriverImpl(tedDriver)

    }

    fun systemId(): String {
        return tedDriverImpl.context.config.systemId()
    }

    fun instanceId(): String {
        return tedDriverImpl.context.config.instanceId()
    }

    fun primeTaskId(): Long? {
        return tedDriverImpl.context.prime.primeTaskId()
    }

    fun dataSource(): DataSource {
        return tedDriverImpl.dataSource
    }

    fun dbType() : DbType {
        return tedDriverImpl.context.tedDao.dbType;
    }
}
