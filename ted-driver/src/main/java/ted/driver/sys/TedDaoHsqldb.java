package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;

import javax.sql.DataSource;

import static java.util.Arrays.asList;
import static ted.driver.sys.JdbcSelectTed.sqlParam;

/**
 * @author Augustus
 * 		   created on 2018.11.03
 *
 * for TED internal usage only!!!
 *
 */
class TedDaoHsqldb extends TedDaoAbstract {
	private static final Logger logger = LoggerFactory.getLogger(TedDaoHsqldb.class);

	TedDaoHsqldb(String system, DataSource dataSource, Stats stats) {
		super(system, dataSource, DbType.HSQLDB, stats);
	}

	@Override
	protected long createTaskInternal(String name, String channel, String data, String key1, String key2, Long batchId, int postponeSec, TedStatus status) {
		String sqlLogId = "create_taskH";
		Long nextId = getSequenceNextValue("SEQ_TEDTASK_ID");
		if (status == null)
			status = TedStatus.NEW;
		String nextts = (status == TedStatus.NEW ? dbType.sql.now() + " + " + dbType.sql.intervalSeconds(postponeSec) : "null");

		String sql = " insert into tedtask (taskId, system, name, channel, bno, status, createTs, nextTs, retries, data, key1, key2, batchId)" +
				" values(?, '$sys', ?, ?, null, '$status', $now, $nextts, 0, ?, ?, ?, ?)";
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$nextts", nextts);
		sql = sql.replace("$status", status.toString());

		execute(sqlLogId, sql, asList(
				sqlParam(nextId, JetJdbcParamType.LONG),
				sqlParam(name, JetJdbcParamType.STRING),
				sqlParam(channel, JetJdbcParamType.STRING),
				sqlParam(data, JetJdbcParamType.STRING),
				sqlParam(key1, JetJdbcParamType.STRING),
				sqlParam(key2, JetJdbcParamType.STRING),
				sqlParam(batchId, JetJdbcParamType.LONG)
		));
		logger.trace("Task {} {} created successfully. ", name, nextId);
		return nextId;

	}}
