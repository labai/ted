package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.JdbcSelectTed.ExecInConn;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.JdbcSelectTed.SqlParam;
import ted.driver.sys.JdbcSelectTed.TedSqlDuplicateException;
import ted.driver.sys.JdbcSelectTed.TedSqlException;
import ted.driver.sys.Model.TaskParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.PrimeInstance.CheckPrimeParams;
import ted.driver.sys.QuickCheck.CheckResult;
import ted.driver.sys.QuickCheck.GetWaitChannelsResult;
import ted.driver.sys.SqlUtils.DbType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static ted.driver.sys.JdbcSelectTed.sqlParam;
import static ted.driver.sys.MiscUtils.asList;

/**
 * @author Augustus
 *         created on 2017.04.14
 *
 * common functions for all implementations (Oracle, Postgres, MySql, Hsqldb)
 *
 * for TED internal usage only!!!
 */
abstract class TedDaoAbstract implements TedDao {
	private static final Logger logger = LoggerFactory.getLogger(TedDaoAbstract.class);

	private Random random = new Random();

	protected final String thisSystem;
	protected final DataSource dataSource;
	protected final Stats stats;
	protected final DbType dbType;
	protected final String systemCheck;

	// if something is wrong with batch updates, then can disable them
	private boolean isSqlBatchUpdateDisabled = false;
	public void setSqlBatchUpdateDisabled(boolean sqlBatchUpdateDisabled) { this.isSqlBatchUpdateDisabled = sqlBatchUpdateDisabled; }

	public TedDaoAbstract(String system, DataSource dataSource, DbType dbType, Stats stats) {
		this.thisSystem = system;
		this.dataSource = dataSource;
		this.dbType = dbType;
		this.stats = stats;
		this.systemCheck = dbType.sql().systemColumn() + " = '" + thisSystem + "'"; // add to queries [system = 'thisSystem']
	}

	@Override
	public DbType getDbType() {
		return dbType;
	}

	@Override
	public Long createTask(String name, String channel, String data, String key1, String key2, Long batchId, Connection conn) {
		return createTaskInternal(name, channel, data, key1, key2, batchId, 0, TedStatus.NEW, conn);
	}

	@Override
	public Long createTaskPostponed(String name, String channel, String data, String key1, String key2, int postponeSec, Connection conn) {
		return createTaskInternal(name, channel, data, key1, key2, null, postponeSec, TedStatus.NEW, conn);
	}

	@Override
	public Long createTaskWithWorkStatus(String name, String channel, String data, String key1, String key2, Connection conn) {
		return createTaskInternal(name, channel, data, key1, key2, null, 0, TedStatus.WORK, conn);
	}

	abstract protected long createTaskInternal(String name, String channel, String data, String key1, String key2, Long batchId, int postponeSec, TedStatus status, Connection conn);

	@Override
	public List<GetWaitChannelsResult> getWaitChannels() {
		String sqlLogId = "get_wait_chan";
		String sql = "select channel, count(*) as taskCnt from tedtask where $systemCheck and nextTs <= $now group by channel";
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);
		List<GetWaitChannelsResult> chans = selectData(sqlLogId, sql, GetWaitChannelsResult.class, Collections.emptyList());
		return chans;
	}

	@Override
	public void setStatuses(List<SetTaskStatus> statuses) {
		if (statuses.isEmpty())
			return;

		// Final status - DONE, ERROR
		// NonFinal - RETRY, NEW, WORK
		String sql = "update tedtask" +
				" set status = ?, msg = ?," +
				" retries = retries + ?," +
				" nextTs = ?" +
				" where $systemCheck and taskId = ?";
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);

		List<List<SqlParam>> params = statuses.stream().map(it -> asList(
				sqlParam(it.status.toString(), JetJdbcParamType.STRING),
				sqlParam(it.msg, JetJdbcParamType.STRING),
				sqlParam((it.status == TedStatus.RETRY ? 1 : 0), JetJdbcParamType.LONG),
				sqlParam((it.status == TedStatus.DONE || it.status == TedStatus.ERROR ? null : it.nextRetryTs), JetJdbcParamType.TIMESTAMP),
				sqlParam(it.taskId, JetJdbcParamType.LONG)
				)).collect(Collectors.toList());

		executeBatch(null, "set_status", sql, params);
	}

	@Override
	public List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes){
		assert dbType != DbType.ORACLE;
		assert dbType != DbType.POSTGRES;

		if (channelSizes.isEmpty())
			return Collections.emptyList();

		long bno;
		if (dbType == DbType.HSQLDB)
			bno = getSequenceNextValue("SEQ_TEDTASK_BNO");
		else if (dbType == DbType.MYSQL)
			bno = Math.abs(random.nextLong()); // we will use random instead of sequences
		else
			throw new IllegalStateException("For Oracle, PostgreSql should be override");

		for (String channel : channelSizes.keySet()) {
			int cnt = channelSizes.get(channel);
			if (cnt == 0) continue;
			reserveTaskPortionForChannel(bno, channel, cnt);
		}
		String sql = "select * from tedtask where bno = ?";
		List<TaskRec> tasks = selectData("get_tasks_by_bno", sql, TaskRec.class, asList(
				sqlParam(bno, JetJdbcParamType.LONG)
		));
		return tasks;
	}


	private void reserveTaskPortionForChannel(long bno, String channel, int rowLimit) {
		String sqlLogId = "reserve_channel";
		String sql = "update tedtask set status = 'WORK', bno = ?, startTs = $now, nextTs = null"
				+ " where status in ('NEW','RETRY') and $systemCheck"
				+ " and taskid in (select taskid from ("
					+ " select taskid from tedtask "
					+ " where status in ('NEW','RETRY') and $systemCheck and channel = ?"
					+ " and nextTs < $now"
					+ " limit ?"
					+ " $FOR_UPDATE_SKIP_LOCKED"
					+ ") tmp"
				// + dbType.sql.rownum("" + rowLimit)
				+ ")"
				;
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);
		sql = sql.replace("$FOR_UPDATE_SKIP_LOCKED", dbType.sql().forUpdateSkipLocked());

		execute(sqlLogId, sql, asList(
				sqlParam(bno, JetJdbcParamType.LONG),
				sqlParam(channel, JetJdbcParamType.STRING),
				sqlParam(rowLimit, JetJdbcParamType.INTEGER)
		));
	}


	// for postgres will be override
	@Override
	public List<CheckResult> quickCheck(CheckPrimeParams checkPrimeParams, boolean skipChannelCheck) {
		if (checkPrimeParams != null)
			throw new IllegalStateException("Prime supported only for PostgreSql");
		if (skipChannelCheck)
			return Collections.emptyList();
		List<GetWaitChannelsResult> chans = getWaitChannels();
		List<CheckResult> res = new ArrayList<>();
		for (GetWaitChannelsResult chan : chans) {
			res.add(new CheckResult("CHAN", chan.channel, chan.taskCnt));
		}
		return res;
	}

	@Override
	public void processMaintenanceRare(int deleteAfterDays) {
		String sql;

		//  update channel null to MAIN (is it necessary?)
		sql = "update tedtask set channel = 'MAIN' where channel is null and $systemCheck and status = 'NEW'";
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);
		execute("maint03", sql, Collections.emptyList());

		if (deleteAfterDays < 99999) {
			// delete finished tasks > 35 days old
			sql = "delete from tedtask where $systemCheck and status in ('ERROR', 'DONE')" +
					" and createTs < ($now - $days35) and finishTs < ($now - $days35)";
			sql = sql.replace("$now", dbType.sql().now());
			sql = sql.replace("$systemCheck", systemCheck);
			sql = sql.replace("$days35", dbType.sql().intervalDays(deleteAfterDays));
			execute("delold", sql, Collections.emptyList());
		}
	}

	@Override
	public void processMaintenanceFrequent() {
		// those tedtask with finishTs not null and finishTs < now goes to RETRY
		// (here finishTs is maximum time, until task expected to be executed)
		String sql = "update tedtask" +
				" set status = 'RETRY', finishTs = null, nextTs = $now, msg = '" + Model.TIMEOUT_MSG + "(2)', retries = retries + 1" +
				" where status = 'WORK' and startTs < ($now - $seconds60) and (finishTs is not null and finishTs < $now)";
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);
		sql = sql.replace("$seconds60", dbType.sql().intervalSeconds(60));
		execute("maint01", sql, Collections.emptyList());

		//  update NEW w/o nextTs
		sql = "update tedtask set nextTs = $now where status in ('NEW', 'RETRY') and $systemCheck and nextTs is null";
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);
		execute("maint02", sql, Collections.emptyList());

		if (dbType == DbType.POSTGRES) { // eventsQueue is not for Oracle
			// find queue events w/o head
			sql = "with headless as (" +
					" select taskid, key1 from tedtask t1 where channel = 'TedEQ' and status = 'SLEEP' and $systemCheck" +
					" and createts < $now - $seconds10" +
					" and not exists (select taskid from tedtask t2 where channel = 'TedEQ' " +
					"   and status in ('NEW', 'RETRY', 'WORK', 'ERROR')" +
					"   and t2.key1 = t1.key1 and t2.system = t1.system)" +
					")" +
					" update tedtask set status = 'NEW', nextTs = $now " +
					" where taskid in (select min(taskid) taskid from headless group by key1)";
			sql = sql.replace("$now", dbType.sql().now());
			sql = sql.replace("$systemCheck", systemCheck);
			sql = sql.replace("$seconds10", dbType.sql().intervalSeconds(10));
			execute("maint04", sql, Collections.emptyList());
		}

		//  update finished statuses (DONE, ERROR) with nextTs (should not happen, may occurs during dev)
		sql = "update tedtask set nextTs = null where status in ('DONE', 'ERROR') and $systemCheck and nextTs < $now - $delta";
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);
		sql = sql.replace("$delta", dbType.sql().intervalSeconds(120));
		execute("maint05", sql, Collections.emptyList());
	}

	@Override
	public List<TaskRec> getWorkingTooLong() {
		// get all working tasks with more than 1min for further analysis (except those, with planed finishTs > now)
		String sql = "select * from tedtask where $systemCheck and status = 'WORK'" +
				" and startTs < ($now - $seconds60) and (finishTs is null or finishTs <= $now)";
		sql = sql.replace("$now", dbType.sql().now());
		sql = sql.replace("$systemCheck", systemCheck);
		sql = sql.replace("$seconds60", dbType.sql().intervalSeconds(60));
		List<TaskRec> tasks = selectData("get_working_too_long", sql, TaskRec.class, Collections.<SqlParam>emptyList());
		return tasks;
	}

	@Override
	public void setTaskPlannedWorkTimeout(long taskId, Date timeoutTime) {
		String sqlLogId = "set_task_work_timeout";
		String sql = "update tedtask set finishTs = ? where taskId = ? and status = 'WORK'";
		execute(sqlLogId, sql, asList(
				sqlParam(timeoutTime, JetJdbcParamType.TIMESTAMP),
				sqlParam(taskId, JetJdbcParamType.LONG)
		));
	}

	@Override
	public TaskRec getTask(long taskId) {
		String sqlLogId = "get_task";
		String sql = "select * from tedtask where taskId = ?";
		List<TaskRec> results = selectData(sqlLogId, sql, TaskRec.class, asList(
				sqlParam(taskId, JetJdbcParamType.LONG)
		));
		return results.isEmpty() ? null : results.get(0);
	}

	// is not really bulk...
	// for postgres will be override
	@Override
	public List<Long> createTasksBulk(List<TaskParam> taskParams) {
		assert dbType != DbType.POSTGRES;
		ArrayList<Long> taskIds = new ArrayList<Long>();
		for (TaskParam param : taskParams) {
			Long taskId = createTask(param.name, param.channel, param.data, param.key1, param.key2, param.batchId, null);
			taskIds.add(taskId);
		}
		return taskIds;
	}


	@Override
	public boolean checkIsBatchFinished(long batchId) {
		String sqlLogId = "check_batch";
		String sql = "select taskid from tedtask where $systemCheck"
				+ " and status in ('NEW', 'RETRY', 'WORK')"
				+ " and batchid = ?"
				+ dbType.sql().rownum(1)
				;
		sql = sql.replace("$systemCheck", systemCheck);
		List<TaskRec> results = selectData(sqlLogId, sql, TaskRec.class, asList(
				sqlParam(batchId, JetJdbcParamType.LONG)
		));
		return results.isEmpty();
	}

	@Override
	public void cleanupBatchTask(Long taskId, String msg, String channel) {
		String sqlLogId = "clean_retry";
		String sql = "update tedtask set retries = 0, msg = ?, channel = ? where taskId = ?";
		execute(sqlLogId, sql, asList(
				sqlParam(msg, JetJdbcParamType.STRING),
				sqlParam(channel, JetJdbcParamType.STRING),
				sqlParam(taskId, JetJdbcParamType.LONG)
		));
	}


	protected Long getSequenceNextValue(String seqName) {
		return selectSingleLong("get_sequence", dbType.sql().sequenceSelect(seqName));
	}

	//
	// wrapper
	//

	protected void execute(String sqlLogId, final String sql, final List<SqlParam> params) {
		execute(null, sqlLogId, sql, params);
	}

	protected void execute(Connection conn, String sqlLogId, final String sql, final List<SqlParam> params) {
		smartRunWithLog(conn, sqlLogId, conn1 -> {
			JdbcSelectTedImpl.executeUpdate(conn1, sql, params);
			return true;
		});
	}

	protected void executeBatch(Connection conn, String sqlLogId, final String sql, final List<List<SqlParam>> params) {
		smartRunWithLog(conn, sqlLogId, conn1 -> {
			if (params.size() == 1) {
				int res = JdbcSelectTedImpl.executeUpdate(conn1, sql, params.get(0));
				return asList(res);
			} else if (this.isSqlBatchUpdateDisabled) {
				List<Integer> res = new ArrayList<>();
				logger.debug("sqlBatchUpdate is disabled, executing in loop");
				for (List<SqlParam> par : params) {
					int res1 = JdbcSelectTedImpl.executeUpdate(conn1, sql, par);
					res.add(res1);
				}
				return res;
			} else {
				return JdbcSelectTedImpl.executeBatchUpdate(conn1, sql, params);
			}
		});
	}

	protected <T> List<T> selectData(String sqlLogId, final String sql, final Class<T> clazz, final List<SqlParam> params) {
		return runInConnWithLog(sqlLogId, conn -> JdbcSelectTedImpl.selectData(conn, sql, clazz, params));
	}

	protected Long selectSingleLong(String sqlLogId, String sql) {
		return selectSingleLong(sqlLogId, sql, null);
	}

	private Long selectSingleLong(String sqlLogId, final String sql, final List<SqlParam> params) {
		return runInConnWithLog(sqlLogId,
				conn -> JdbcSelectTedImpl.selectSingleLong(conn, sql, params));
	}

	protected <T> T runInConnWithLog(String sqlLogId, ExecInConn<T> executor) {
		return wrapWithLog(sqlLogId,
				() -> JdbcSelectTed.runInConn(dataSource, executor));
	}

	// "smart" conn check - is conn is null, then will take from dataSource
	protected <T> T smartRunWithLog(Connection conn, String sqlLogId, ExecInConn<T> executor) {
		if (conn == null) {
			return wrapWithLog(sqlLogId, () -> JdbcSelectTed.runInConn(dataSource, executor));
		} else {
			// when want to execute in existing connection
			return wrapWithLog(sqlLogId, () -> {
				try {
					return executor.execute(conn);
				} catch (SQLException e) {
					logger.error("Sql exception: " + e.getMessage());
					throw new TedSqlException(e);
				}
			});
		}
	}

	private  <T> T wrapWithLog(String sqlLogId, Supplier<T> executor) {
		long startTm = System.currentTimeMillis();
		T result = null;

		try {
			result = executor.get();
			return result;
		} catch (TedSqlException e) {
			if (e.getCause() != null && e.getCause() instanceof SQLException) {
				handleSQLException((SQLException) e.getCause(), sqlLogId);
			}
			throw e;
		} finally {
			long durationMs = System.currentTimeMillis() - startTm;
			int resCnt;
			if (result == null)
				resCnt = 0;
			else if (result instanceof Collection)
				resCnt = ((Collection) result).size();
			else {
				resCnt = 1;
			}
			stats.metrics.dbCall(sqlLogId, resCnt, (int)durationMs);
		}
	}

	private void handleSQLException(SQLException sqle, String sqlLogId) {
		if ("23505".equals(sqle.getSQLState())) { // 23505 in postgres: duplicate key value violates unique constraint. TODO for oracle - don't care(?)
			logger.info("duplicate was found for unique index. sqlId={} message={}", sqlLogId, sqle.getMessage());
			throw new TedSqlDuplicateException("duplicate was found for sqlId=" + sqlLogId, sqle);
		}
		logger.error("SQLException while execute '{}': {}", sqlLogId, sqle.getMessage());
		throw new TedSqlException("SQL exception while calling sqlId '" + sqlLogId + "'", sqle);
	}
}
