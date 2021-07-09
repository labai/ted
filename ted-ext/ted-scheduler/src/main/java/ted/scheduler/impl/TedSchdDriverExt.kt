package ted.scheduler.impl


import ted.driver.TedDriver
import ted.driver.sys.SqlUtils.DbType
import ted.driver.sys._SchdExt
import javax.sql.DataSource

/**
 * @author Augustus
 * created on 2018.08.17
 *
 * use some of internal functions.
 *
 * For ted-scheduler only!
 */
class TedSchdDriverExt(tedDriver: TedDriver) : _SchdExt(tedDriver) {
    public override fun isPrimeEnabled() = super.isPrimeEnabled()
    public override fun systemId(): String = super.systemId()
    public override fun instanceId(): String = super.instanceId()
    public override fun primeTaskId(): Long? = super.primeTaskId()
    public override fun dataSource(): DataSource = super.dataSource()
    public override fun dbType(): DbType = super.dbType()
    public override fun tableName(): String = super.tableName()
    public override fun schemaName(): String? = super.schemaName()
    public override fun getPropertyValue(key: String): String? = super.getPropertyValue(key)
}
