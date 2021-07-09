package ted.driver.sys;

import ted.driver.TedDriver;
import ted.driver.sys.SqlUtils.DbType;

import javax.sql.DataSource;

/**
 * @author Augustus
 * created on 2018.08.17
 *
 * use some of internal functions.
 *
 * For ted-scheduler only!
 */
public abstract class _SchdExt extends ted.driver._SchdExt {
    public _SchdExt(TedDriver tedDriver) { super(tedDriver); }
    protected boolean isPrimeEnabled() { return tedDriverImpl.prime().isEnabled(); }
    protected String systemId() { return tedDriverImpl.getContext().config.systemId(); }
    protected String instanceId() { return tedDriverImpl.getContext().config.instanceId(); }
    protected Long primeTaskId() { return tedDriverImpl.getContext().prime.primeTaskId(); }
    protected DataSource dataSource() { return tedDriverImpl.dataSource; }
    protected DbType dbType() { return tedDriverImpl.getContext().tedDao.getDbType(); }
    protected String tableName() { return tedDriverImpl.getContext().config.tableName(); }
    protected String schemaName() { return tedDriverImpl.getContext().config.schemaName(); }
    protected String getPropertyValue(String key) { return tedDriverImpl.getContext().config.getPropertyValue(key); }
}
