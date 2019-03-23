package ted.driver.sys;

import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.JdbcSelectTed.SqlParam;
import ted.driver.sys.TedDaoAbstract.DbType;
import ted.driver.sys.TedDriverImpl.TedContext;
import org.junit.Assume;
import org.junit.Before;

import java.util.Collections;

import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public abstract class TestBase {

	protected abstract TedDriverImpl getDriver();

	@Before
	public void initCheck() {
		Assume.assumeTrue("Are tests enabled?", TestConfig.INT_TESTS_ENABLED);

	}

	protected TedContext getContext() {
		return getDriver().getContext();
	}

	protected void dao_cleanupAllTasks() {
		DbType dbType = getDriver().getContext().tedDao.getDbType();
		((TedDaoAbstract)getContext().tedDao).execute("dao_cleanupTasks",
				" update tedtask set status = 'ERROR', nextTs = null, msg = concat('cleanup from status ', status) " +
						" where "+ dbType.sql.systemColumn() +" = '" + TestConfig.SYSTEM_ID + "' and status in ('NEW', 'WORK', 'RETRY')", Collections.<SqlParam>emptyList());
	}

	protected void dao_cleanupTasks(String taskName) {
		DbType dbType = getDriver().getContext().tedDao.getDbType();
		((TedDaoAbstract)getContext().tedDao).execute("dao_cleanupTasks",
				" update tedtask set status = 'ERROR', nextTs = null, msg = concat('cleanup from status ', status) " +
						" where "+ dbType.sql.systemColumn() +" = '" + TestConfig.SYSTEM_ID + "' and status in ('NEW', 'WORK', 'RETRY') and name = ?", asList(
						JdbcSelectTed.sqlParam(taskName, JetJdbcParamType.STRING)
				));
	}

	protected void dao_cleanupPrime() {
		DbType dbType = getDriver().getContext().tedDao.getDbType();
		((TedDaoAbstract)getContext().tedDao).execute("dao_cleanupPrime",
					" update tedtask set finishTs = null"
							+ " where "+ dbType.sql.systemColumn() +" = '" + TestConfig.SYSTEM_ID + "' "
							+ " and name = 'TED_PRIME'", Collections.<SqlParam>emptyList());
	}

}
