package com.github.ted.sys;

import com.github.ted.Ted.TedStatus;
import com.github.ted.sys.JdbcSelectTed.JetJdbcParamType;
import com.github.ted.sys.JdbcSelectTed.SqlParam;
import com.github.ted.sys.Model.TaskParam;
import com.github.ted.sys.Model.TaskRec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static com.github.ted.sys.JdbcSelectTed.sqlParam;

/**
 * @author Augustus
 *         created on 2017.04.14
 *
 * common functions for all implementations (Oracle, Postgres)
 *
 * for TED internal usage only!!!
 */
abstract class TedDaoAbstract implements TedDao {
	private static final Logger logger = LoggerFactory.getLogger(TedDaoAbstract.class);

	protected interface SqlDbExt {
		String now();
		String intervalSeconds(int secCount);
		String intervalDays(int dayCount);
		String rownum(String rowNum);
		String sequenceSql(String seqName);
	}

	protected enum DbType {
		ORACLE(new SqlDbExt() {
			public String now() { return "systimestamp"; }
			public String intervalSeconds(int secCount) { return secCount + " / 86400"; }
			public String intervalDays(int dayCount) { return "" + dayCount; }
			public String rownum(String rowNum) { return " and rownum <= " + rowNum; } // must be last of conditions
			public String sequenceSql(String seqName) { return "select " + seqName + ".nextval from dual"; }
		}),
		POSTGRES(new SqlDbExt() {
			public String now() { return "now()"; }
			public String intervalSeconds(int secCount) { return "interval '" + secCount + " second'"; }
			public String intervalDays(int dayCount) { return "interval '" + dayCount + " day'";}
			public String rownum(String rowNum) { return " limit " + rowNum; }
			public String sequenceSql(String seqName) { return "select nextval('" + seqName + "')"; }
		});
		protected final SqlDbExt sql;

		DbType(SqlDbExt sql) {
			this.sql = sql;
		}
	}

	protected final String thisSystem;
	protected final DataSource dataSource;
	protected final DbType dbType;

	public TedDaoAbstract(String system, DataSource dataSource, DbType dbType) {
		this.thisSystem = system;
		this.dataSource = dataSource;
		this.dbType = dbType;
	}

	@Override
	public DbType getDbType() {
		return dbType;
	}

	@Override
	public Long createTask(String name, String channel, String data, String key1, String key2, Long batchId) {
		Long nextId = getSequenceNextValue("SEQ_TEDTASK_ID");
		createTaskWithId(nextId, name, channel, data, key1, key2, batchId, 0);
		return nextId;
	}

	@Override
	public Long createTaskPostponed(String name, String channel, String data, String key1, String key2, int postponeSec) {
		Long nextId = getSequenceNextValue("SEQ_TEDTASK_ID");
		createTaskWithId(nextId, name, channel, data, key1, key2, null, postponeSec);
		return nextId;
	}

	protected void createTaskWithId(Long taskId, String name, String channel, String data, String key1, String key2, Long batchId, int postponeSec) {
		String sqlLogId = "create_task";
		String sql = " insert into tedtask (taskId, system, name, channel, bno, status, createTs, nextTs, retries, data, key1, key2, batchId)" +
				" values(?, '$sys', ?, ?, null, 'NEW', $now, $now + $postpone, 0, ?, ?, ?, ?)";
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$postpone", dbType.sql.intervalSeconds(postponeSec));

		execute(sqlLogId, sql, asList(
				JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG),
				JdbcSelectTed.sqlParam(name, JetJdbcParamType.STRING),
				JdbcSelectTed.sqlParam(channel, JetJdbcParamType.STRING),
				JdbcSelectTed.sqlParam(data, JetJdbcParamType.STRING),
				JdbcSelectTed.sqlParam(key1, JetJdbcParamType.STRING),
				JdbcSelectTed.sqlParam(key2, JetJdbcParamType.STRING),
				JdbcSelectTed.sqlParam(batchId, JetJdbcParamType.LONG)
		));
		logger.trace("Task " + name + " created successfully.");
		return;
	}


	public List<Long> createTasksBulk(List<TaskParam> taskParams) {
		if (dbType != DbType.POSTGRES)
			throw new IllegalArgumentException("this method is allowed only for PostgreSql db");
		throw new IllegalArgumentException("method must be overridden");
	}

	private static class ChannelRes {
		String channel;
	}

	@Override
	public List<String> getWaitChannels() {
		String sqlLogId = "get_wait_chan";
		String sql = "select distinct channel from tedtask where system = '$sys' and nextTs <= $now";
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		List<ChannelRes> chans = selectData(sqlLogId, sql, ChannelRes.class, Collections.<SqlParam>emptyList());
		List<String> result = new ArrayList<String>();
		for (ChannelRes channelRes : chans)
			result.add(channelRes.channel);
		return result;
	}

	// TODO now it requires minimum 3 calls to db
	@Override
	public List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes){
		if (channelSizes.isEmpty())
			return Collections.emptyList();
		long bno = getSequenceNextValue("SEQ_TEDTASK_BNO");

		for (String channel : channelSizes.keySet()) {
			int cnt = channelSizes.get(channel);
			if (cnt == 0) continue;
			reserveTaskPortionForChannel(bno, channel, cnt);
		}
		String sql = "select * from tedtask where bno = ?";
		List<TaskRec> tasks = selectData("get_tasks_by_bno", sql, TaskRec.class, asList(
				JdbcSelectTed.sqlParam(bno, JetJdbcParamType.LONG)
		));
		return tasks;
	}

	@Override
	public void setStatus(long taskId, TedStatus status, String msg) {
		setStatusPostponed(taskId, status, msg, null);
	}

	@Override
	public void setStatusPostponed(long taskId, TedStatus status, String msg, Date nextRetryTs) {
		String sqlLogId = "set_status";
		String sql;
/*
		String sql = "update tedtask" +
				" set status = :p_status, msg = :p_msg, " +
				"     retries = (case when p_status = 'RETRY' then retries + 1 else retries end)," +
				"     nextTs = (case when p_status = 'RETRY' then :p_nextTs else null end)," +
				"     finishTs = (case when p_status in ('DONE', 'ERROR') then $now else finishTs end)" +
				" where system = '$sys' and taskId = :p_task_id";
*/

		// Final status - DONE, ERROR
		if (status == TedStatus.DONE || status == TedStatus.ERROR) {

			sql = "update tedtask" +
					" set status = ?, msg = ?," +
					"     nextTs = null, finishTs = $now" +
					" where system = '$sys' and taskId = ?";
			sql = sql.replace("$now", dbType.sql.now());
			sql = sql.replace("$sys", thisSystem);
			execute(sqlLogId, sql, asList(
					JdbcSelectTed.sqlParam(status.toString(), JetJdbcParamType.STRING),
					JdbcSelectTed.sqlParam(msg, JetJdbcParamType.STRING),
					JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
			));

		// RETRY, NEW, WORK
		} else {

			sql = "update tedtask" +
					" set status = ?, msg = ?, " +
					(status == TedStatus.RETRY ? " retries = retries + 1," : "") +
					" 	nextTs = ?" +
					" where system = '$sys' and taskId = ?";
			sql = sql.replace("$now", dbType.sql.now());
			sql = sql.replace("$sys", thisSystem);
			execute(sqlLogId, sql, asList(
					JdbcSelectTed.sqlParam(status.toString(), JetJdbcParamType.STRING),
					JdbcSelectTed.sqlParam(msg, JetJdbcParamType.STRING),
					JdbcSelectTed.sqlParam(nextRetryTs, JetJdbcParamType.TIMESTAMP),
					JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
			));
		}
	}

	@Override
	public void processMaintenanceRare(int deleteAfterDays) {
		String sql;

		//  update channel null to MAIN (is it necessary?)
		sql = "update tedtask set channel = 'MAIN' where channel is null and system = '$sys' and status = 'NEW'";
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		execute("maint03", sql, Collections.<SqlParam>emptyList());

		if (deleteAfterDays < 99999) {
			// delete finished tasks > 35 days old
			sql = "delete from tedtask where system = '$sys' and status in ('ERROR', 'DONE')" +
					" and createTs < ($now - $days35) and finishTs < ($now - $days35)";
			sql = sql.replace("$now", dbType.sql.now());
			sql = sql.replace("$sys", thisSystem);
			sql = sql.replace("$days35", dbType.sql.intervalDays(deleteAfterDays));
			execute("delold", sql, Collections.<SqlParam>emptyList());
		}
	}

	@Override
	public void processMaintenanceFrequent() {
		// those tedtask with finishTs not null and finishTs < now goes to RETRY
		// (here finishTs is maximum time, until task expected to be executed)
		String sql = "update tedtask" +
				" set status = 'RETRY', finishTs = null, nextTs = $now, msg = 'Too long in status [work](2)', retries = retries + 1" +
				" where status = 'WORK' and startTs < ($now - $seconds60) and (finishTs is not null and finishTs < $now)";
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$seconds60", dbType.sql.intervalSeconds(60));
		execute("maint01", sql, Collections.<SqlParam>emptyList());

		//  update NEW w/o nextTs
		sql = "update tedtask set nextTs = $now where status in ('NEW', 'RETRY') and system = '$sys' and nextTs is null";
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		execute("maint02", sql, Collections.<SqlParam>emptyList());

	}

	@Override
	public List<TaskRec> getWorkingTooLong() {
		// get all working tasks with more than 1min for further analysis (except those, with planed finishTs > now)
		String sql = "select * from tedtask where system = '$sys' and status = 'WORK'" +
				" and startTs < ($now - $seconds60) and (finishTs is null or finishTs <= $now)";
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$seconds60", dbType.sql.intervalSeconds(60));
		List<TaskRec> tasks = selectData("get_working_too_long", sql, TaskRec.class, Collections.<SqlParam>emptyList());
		return tasks;
	}

	@Override
	public void setTaskPlannedWorkTimeout(long taskId, Date timeoutTime) {
		String sqlLogId = "set_task_work_timeout";
		String sql = "update tedtask set finishTs = ? where taskId = ? and status = 'WORK'";
		execute(sqlLogId, sql, asList(
				JdbcSelectTed.sqlParam(timeoutTime, JetJdbcParamType.TIMESTAMP),
				JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
		));
	}

	@Override
	public TaskRec getTask(long taskId) {
		String sqlLogId = "get_task";
		String sql = "select * from tedtask where taskId = ?";
		List<TaskRec> results = selectData(sqlLogId, sql, TaskRec.class, asList(
				JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
		));
		if (results.size() == 0)
			throw new RuntimeException("No task was for taskid=" + taskId);
		if (results.size() > 1)
			throw new RuntimeException("Expected only 1 record, but found " + results.size() + " for taskid=" + taskId);
		return results.get(0);
	}

	@Override
	public boolean checkIsBatchFinished(long batchId) {
		String sqlLogId = "check_batch";
		String sql = "select taskid from tedtask where system = '$sys'"
				+ " and status in ('NEW', 'RETRY', 'WORK')"
				+ " and batchid = ?"
				+ dbType.sql.rownum("1")
				;
		sql = sql.replace("$sys", thisSystem);
		List<TaskRec> results = selectData(sqlLogId, sql, TaskRec.class, asList(
				JdbcSelectTed.sqlParam(batchId, JetJdbcParamType.LONG)
		));
		return results.isEmpty();
	}

	@Override
	public void cleanupRetries(Long taskId, String msg) {
		String sqlLogId = "clean_retry";
		String sql = "update tedtask set retries = 0, msg = ? where taskId = ?";
		execute(sqlLogId, sql, asList(
				JdbcSelectTed.sqlParam(msg, JetJdbcParamType.STRING),
				JdbcSelectTed.sqlParam(taskId, JetJdbcParamType.LONG)
		));
	}

	private static class StatsRes {
		String status;
		Integer cnt;
	}

	@Override
	public Map<TedStatus, Integer> getBatchStatusStats(long batchId) {
		String sqlLogId = "batch_stats";
		String sql = "select status, count(*) as cnt from tedtask where system = '$sys'"
				+ " and batchid = ?"
				+ " group by status";
		sql = sql.replace("$sys", thisSystem);
		List<StatsRes> statsResults = selectData(sqlLogId, sql, StatsRes.class, asList(
				JdbcSelectTed.sqlParam(batchId, JetJdbcParamType.LONG)
		));
		Map<TedStatus, Integer> resMap = new HashMap<TedStatus, Integer>();
		for (StatsRes stats : statsResults) {
			TedStatus status = TedStatus.valueOf(stats.status);
			resMap.put(status, stats.cnt);
		}
		return resMap;
	}

	//
	// private
	//

	private void reserveTaskPortionForChannel(long bno, String channel, int rowLimit) {
		String sqlLogId = "reserve_channel";
		String sql = "update tedtask set status = 'WORK', bno = ?, startTs = $now, nextTs = null"
				+ " where status in ('NEW','RETRY') and system = '$sys'"
				+ " and taskid in ("
					+ " select taskid from tedtask "
					+ " where status in ('NEW','RETRY') and system = '$sys' and channel = ? "
					+ " and nextTs < $now"
					+ (dbType == DbType.POSTGRES ? " for update skip locked" : "") // "for update skip locked" works for Postgres. For Oracle implemented in TedDaoOracle
					+ dbType.sql.rownum("" + rowLimit) // todo use rowLimit as parameter
					//+ " for update"
				+ ")"
				;
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		execute(sqlLogId, sql, asList(
				JdbcSelectTed.sqlParam(bno, JetJdbcParamType.LONG),
				JdbcSelectTed.sqlParam(channel, JetJdbcParamType.STRING)
				//,sqlParam(rowLimit, JetJdbcParamType.INTEGER)
		));
	}


	protected Long getSequenceNextValue(String seqName) {
		return selectSingleLong("get_sequence", dbType.sql.sequenceSql(seqName));
	}

	//
	// wrapper
	//
	private void logSqlParams(String sqlLogId, String sql, List<SqlParam> params) {
		if (logger.isTraceEnabled()) {
			String sparams = "";
			for(SqlParam p : params)
				sparams += String.format(" %s=%s", p.code, p.value);
			logger.trace("Before[{}] with params:{}", sqlLogId, sparams);
			if (logger.isTraceEnabled()) {
				logger.trace("sql:" + sql);
			}
		}
	}

	protected void execute(String sqlLogId, String sql, List<SqlParam> params) {
		logSqlParams(sqlLogId, sql, params);
		long startTm = System.currentTimeMillis();
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new RuntimeException("Cannot get DB connection", e);
		}

		try {
			JdbcSelectTed.execute(connection, sql, params);
		} catch (SQLException e) {
			logger.error("SQLException while execute '{}': {}. SQL={}", sqlLogId, e.getMessage(), sql);
			throw new RuntimeException("SQL exception while calling sqlId '" + sqlLogId + "'", e);
		}
		logger.debug("After [{}] time={}ms", sqlLogId, (System.currentTimeMillis() - startTm));
	}

	protected <T> List<T> selectData(String sqlLogId, String sql, Class<T> clazz, List<SqlParam> params) {
		logSqlParams(sqlLogId, sql, params);
		long startTm = System.currentTimeMillis();
		List<T> list = null;
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new RuntimeException("Cannot get DB connection", e);
		}
		try {
			list = JdbcSelectTed.selectData(connection, sql, clazz, params);
		} catch (SQLException e) {
			logger.error("SQLException while selectData '{}': {}. SQL={}", sqlLogId, e.getMessage(), sql);
			throw new RuntimeException("SQL exception while calling sqlId '" + sqlLogId + "'", e);
		}

		logger.debug("After [{}] time={}ms items={}", sqlLogId, (System.currentTimeMillis() - startTm), list.size());
		return list;
	}

	protected Long selectSingleLong(String sqlLogId, String sql) {
		logger.trace("Before[{}]", sqlLogId);
		long startTm = System.currentTimeMillis();
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new RuntimeException("Cannot get DB connection", e);
		}
		Long result;
		try {
			result = JdbcSelectTed.selectSingleLong(connection, sql, null);
		} catch (SQLException e) {
			logger.error("SQLException while selectSingleLong '{}': {}. SQL={}", sqlLogId, e.getMessage(), sql);
			throw new RuntimeException("SQL exception while calling sqlId '" + sqlLogId + "'", e);
		}
		logger.debug("After [{}] time={}ms result={}", sqlLogId, (System.currentTimeMillis() - startTm), result);
		return result;
	}

}
