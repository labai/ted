package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.JdbcSelectTed.ExecInConn;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.JdbcSelectTed.SqlParam;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Random;

import static java.util.Arrays.asList;
import static ted.driver.sys.JdbcSelectTed.sqlParam;

/**
 * @author Augustus
 * created on 2018.09.07
 * <p>
 * for TED internal usage only!!!
 */
class TedDaoMysql extends TedDaoAbstract {
	private static final Logger logger = LoggerFactory.getLogger(TedDaoMysql.class);

	private Random random = new Random();

	public TedDaoMysql(String system, DataSource dataSource) {
		super(system, dataSource, DbType.MYSQL);
	}

	//
	// private
	//

	// taskid is autonumber in MySql
	protected long createTaskInternal(final String name, final String channel, final String data, final String key1, final String key2, final Long batchId, int postponeSec, TedStatus status) {
		final String sqlLogId = "create_task";
		if (status == null)
			status = TedStatus.NEW;
		String nextts = (status == TedStatus.NEW ? dbType.sql.now() + " + " + dbType.sql.intervalSeconds(postponeSec) : "null");

		String sql = " insert into tedtask (taskId, `system`, name, channel, bno, status, createTs, nextTs, retries, data, key1, key2, batchId)" +
				" values(null, '$sys', ?, ?, null, '$status', $now, $nextts, 0, ?, ?, ?, ?)" +
				" ";
		sql = sql.replace("$nextTaskId", dbType.sql.sequenceSql("SEQ_TEDTASK_ID"));
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$nextts", nextts);
		sql = sql.replace("$status", status.toString());

		final String finalSql = sql;
		Long taskId = JdbcSelectTed.runInConn(dataSource, new ExecInConn<Long>() {
			@Override
			public Long execute(Connection connection) throws SQLException {
				int res = JdbcSelectTedImpl.executeUpdate(connection, finalSql, asList(
						sqlParam(name, JetJdbcParamType.STRING),
						sqlParam(channel, JetJdbcParamType.STRING),
						sqlParam(data, JetJdbcParamType.STRING),
						sqlParam(key1, JetJdbcParamType.STRING),
						sqlParam(key2, JetJdbcParamType.STRING),
						sqlParam(batchId, JetJdbcParamType.LONG)
				));
				if (res != 1)
					throw new IllegalStateException("expected 1 insert");
				String sql = "select last_insert_id()";
				return JdbcSelectTedImpl.selectSingleLong(connection, sql, Collections.<SqlParam>emptyList());
			}
		});

		logger.trace("Task {} {} created successfully. ", name, taskId);
		return taskId;

	}

}
