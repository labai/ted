package com.github.ted.sys;

import com.github.ted.sys.JdbcSelectTed.JetJdbcParamType;
import com.github.ted.sys.JdbcSelectTed.SqlParam;
import com.github.ted.sys.TedDriverImpl.TedContext;

import java.util.Collections;

import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2016.09.19
 */
public abstract class TestBase {

	protected abstract TedDriverImpl getDriver();

	protected TedContext getContext() {
		return getDriver().getContext();
	}

	protected void dao_cleanupAllTasks() {
		((TedDaoAbstract)getContext().tedDao).execute("dao_cleanupTasks",
				" update tedtask set status = 'ERROR', nextTs = null, msg = 'cleanup from status '|| status " +
						" where system = '" + TestConfig.SYSTEM_ID + "' and status in ('NEW', 'WORK', 'RETRY')", Collections.<SqlParam>emptyList());
	}

	protected void dao_cleanupTasks(String taskName) {
		((TedDaoAbstract)getContext().tedDao).execute("dao_cleanupTasks",
				" update tedtask set status = 'ERROR', nextTs = null, msg = 'cleanup from status '|| status " +
						" where system = '" + TestConfig.SYSTEM_ID + "' and status in ('NEW', 'WORK', 'RETRY') and name = ?", asList(
						JdbcSelectTed.sqlParam(taskName, JetJdbcParamType.STRING)
				));
	}

}
