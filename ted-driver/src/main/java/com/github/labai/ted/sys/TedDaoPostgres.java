package com.github.labai.ted.sys;

import com.github.labai.ted.Ted.TedStatus;
import com.github.labai.ted.sys.JdbcSelectTed.JetJdbcParamType;
import com.github.labai.ted.sys.JdbcSelectTed.SqlParam;
import com.github.labai.ted.sys.JdbcSelectTed.TedSqlDuplicateException;
import com.github.labai.ted.sys.Model.TaskParam;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.PrimeInstance.CheckPrimeParams;
import com.github.labai.ted.sys.QuickCheck.CheckResult;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.github.labai.ted.sys.JdbcSelectTed.sqlParam;
import static com.github.labai.ted.sys.MiscUtils.nvle;
import static java.util.Arrays.asList;

/**
 * @author Augustus
 * created on 2017.04.14
 * <p>
 * for TED internal usage only!!!
 */
class TedDaoPostgres extends TedDaoAbstract {
	private static final Logger logger = LoggerFactory.getLogger(TedDaoPostgres.class);

	public TedDaoPostgres(String system, DataSource dataSource) {
		super(system, dataSource, DbType.POSTGRES);
	}


	private static class TaskIdRes {
		Long taskid;
	}


	@Override
	public List<Long> createTasksBulk(List<TaskParam> taskParams) {
		ArrayList<Long> taskIds = getSequencePortion("SEQ_TEDTASK_ID", taskParams.size());
		int iNum = 0;
		for (TaskParam param : taskParams) {
			param.taskId = taskIds.get(iNum++);
		}

		try {
			executePgCopy(dataSource.getConnection(), taskParams);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return taskIds;
	}

	@Override
	public List<CheckResult> quickCheck(CheckPrimeParams checkPrimeParams) {
		String sql = "";
		String logId = "a";
		if (checkPrimeParams != null) {
			String sqlPrime;
			if (checkPrimeParams.isPrime()) {
				sqlPrime = ""
						+ "with updatedMasterTask as ("
						+ "  update tedtask set finishts = $now + $intervalSec"
						+ "  where taskid = $primeTaskId"
						+ "  and system = '$sys'"
						+ "  and data = '$instanceId'"
						+ "  returning 'PRIME'::text as result"
						+ ")"
						+ " select case when exists(select * from updatedMasterTask) then 'PRIME' else 'LOST_PRIME' end as name, 'PRIM' as type, null::timestamp as tillts";
				logId = "b";
			} else {
				// TODO return tillTs until prime is reserved
				sqlPrime = "select case when finishts < $now then 'CAN_PRIME' else 'NEXT_CHECK' end as name, 'PRIM' as type, finishts as tillts"
						+ " from tedtask"
						+ " where taskid = $primeTaskId and system = '$sys'";
				logId = "c";
			}
			sqlPrime = sqlPrime.replace("$intervalSec", dbType.sql.intervalSeconds(checkPrimeParams.postponeSec()));
			sqlPrime = sqlPrime.replace("$instanceId", checkPrimeParams.instanceId());
			sqlPrime = sqlPrime.replace("$primeTaskId", Long.toString(checkPrimeParams.primeTaskId()));
			sql += sqlPrime + " union ";
		}
		// check for new tasks
		sql += "select distinct channel as name, 'CHAN' as type, null::timestamp as tillts "
				+ " from tedtask"
				+ " where system = '$sys' and nextTs <= $now";
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$now", dbType.sql.now());
		//logger.info(sql);
		return selectData("quick_check(" + logId + ")", sql, CheckResult.class, Collections.<SqlParam>emptyList());
	}

	//
	// prime
	//

	@Override
	public Long findPrimeTaskId() {
		String sql;
		sql = "select taskid from tedtask where system = '$sys' and name = 'TED:PRIME' limit 2";
		sql = sql.replace("$sys", thisSystem);
		List<TaskIdRes> ls2 = selectData("find_primetask", sql, TaskIdRes.class, Collections.<SqlParam>emptyList());
		if (ls2.size() == 1) {
			logger.debug("found primeTaskId={}", ls2.get(0).taskid);
			return ls2.get(0).taskid;
		}
		if (ls2.size() > 1)
			throw new IllegalStateException("found few primeTaskId tasks for system=" + thisSystem + " (name='TED:PRIME'). Ther should be only 1. Please delete them and restart again");

		// not exists yet - try to create new. alternatively it is possible create manually by deployment script.
		// this part will be executed only once per live of system..

		// get next taskid, prefer some from lower numbers (11..99)
		String sqlNextId = "coalesce("
				+ "  nullif(coalesce((select max(taskid) from tedtask where taskid between 10 and 99), 10) + 1, 100),"
				+ "  $sequenceTedTask"
				+ "  )";
		sql = "insert into tedtask(taskid, system, name, status, channel, startts, nextts, msg)"
				+ " values (" + sqlNextId + ", '$sys', 'TED:PRIME', 'SLEEP', '$channel', $now, null, 'This is internal TED pseudo-task for prime check')";
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$channel", Model.CHANNEL_PRIME);
		sql = sql.replace("$sequenceTedTask", dbType.sql.sequenceSql("SEQ_TEDTASK_ID"));

		execute("insert_prime", sql, Collections.<SqlParam>emptyList());

		// check again, to be sure
		sql = "select taskid from tedtask where system = '$sys' and name = 'TED:PRIME'";
		sql = sql.replace("$sys", thisSystem);
		Long taskId = selectSingleLong("find_primetask(2)", sql);
		if (taskId == null)
			throw new IllegalStateException("Something went wrong, please try to create manually 'TED:PRIME' task for system '" + thisSystem + "'");

		return taskId;
	}

	@Override
	public boolean becomePrime(Long primeTaskId, String instanceId) {
		String sql = "update tedtask set data = '$instanceId', finishts = now() + interval '5 seconds'"
				+ " where taskid = (select taskid from tedtask where system = '$sys' "
				+ "  and (finishts < now() or finishts is null) "
				+ "  and taskid = $primeTaskId for update skip locked)"
				+ " returning taskid";
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$primeTaskId", primeTaskId.toString());
		sql = sql.replace("$instanceId", instanceId);

		List<TaskIdRes> res = selectData("take_prime", sql, TaskIdRes.class, Collections.<SqlParam>emptyList());
		return res.size() == 1;
	}

	//
	// queue events
	//


	@Override
	public Long createEvent(String taskName, String discriminator, String data, String key2) {
		return createTaskInternal(taskName, Model.CHANNEL_QUEUE, data, nvle(discriminator), key2, null, 0, TedStatus.SLEEP);
	}

	@Override
	public TaskRec eventQueueMakeFirst(String discriminator) {
		String sql = "update tedtask set status = 'NEW' where system = '$sys'" +
				" and key1 = ? and status = 'SLEEP' and channel = 'TedEQ'" +
				" and taskid = (select min(taskid) from tedtask t2 " +
				"   where system = '$sys' and channel = 'TedEQ' and t2.key1 = ?" +
				"   and status = 'SLEEP')" +
				" and not exists (select taskid from tedtask t3" +
				"   where system = '$sys' and channel = 'TedEQ' and t3.key1 = ?" +
				"   and status in ('NEW', 'RETRY', 'WORK', 'ERROR'))" +
				" returning tedtask.*";
		sql = sql.replace("$sys", thisSystem);
		try {
			List<TaskRec> recs = selectData("event_make_first", sql, TaskRec.class, asList(
					sqlParam(discriminator, JetJdbcParamType.STRING),
					sqlParam(discriminator, JetJdbcParamType.STRING),
					sqlParam(discriminator, JetJdbcParamType.STRING)
			));
			if (recs.size() != 1) {
				//logger.debug("logid='{}' cannot update, does exists taskid={} ?", taskId);
				return null;
			}
			return recs.get(0);
		} catch (TedSqlDuplicateException e) {
			return null;
		}
	}

	@Override
	public List<TaskRec> eventQueueGetTail(String discriminator) {
		String sql = "select * from tedtask where key1 = ?"
				+ " and status = 'SLEEP'"
				+ " and channel = '$channel' and system = '$sys'"
				+ " order by taskid"
				+ " for update nowait"
				+ " limit 100";
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$channel", Model.CHANNEL_QUEUE);
		List<TaskRec> recs;
		try {
			recs = selectData("queue_tail", sql, TaskRec.class, asList(
					sqlParam(discriminator, JetJdbcParamType.STRING)
			));
		} catch (Exception e) {
			logger.info("select queue_tail got exception: {}", e.getMessage());
			return Collections.emptyList();
		}

		return recs;
	}

	//
	// private
	//

	private void executePgCopy(Connection connection, List<TaskParam> taskParams) throws SQLException {
		String sql = "COPY tedtask (taskId, system, name, channel, status, key1, key2, batchId, data)" +
				" FROM STDIN " +
				" WITH (FORMAT text, DELIMITER '\t', ENCODING 'UTF-8')";

		StringBuilder stringBuilder = new StringBuilder(512);
		for (TaskParam task : taskParams) {
			String str = task.taskId
					+ "\t" + thisSystem
					+ "\t" + task.name
					+ "\t" + task.channel
					+ "\t" + TedStatus.NEW
					+ "\t" + formatStr(task.key1)
					+ "\t" + formatStr(task.key2)
					+ "\t" + task.batchId
					+ "\t" + formatStr(task.data)
					+ "\n";
			stringBuilder.append(str);
		}
		//System.out.println(stringBuilder.toString());
		CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
		try {
			copyManager.copyIn(sql, new StringReader(stringBuilder.toString()));
		} catch (IOException e) {
			throw new SQLException("Unable to execute COPY operation", e);
		}
	}

	private static String formatStr(String str) {
		if (str == null) return "";
		return str.replace("\\", "\\\\").replace("\t", "\\t").replace("\r", "\\r").replace("\n", "\\n");
	}

	private static class ResSeqVal {
		Long seqval;
	}

	/* postgres only */
	protected ArrayList<Long> getSequencePortion(String seqName, int number) {
		if (number < 1 || number > 100000)
			throw new IllegalArgumentException("Invalid requested sequence count: " + number);
		String sql = "select nextval('" + seqName + "') as seqval from generate_series(1," + number + ")";
		List<ResSeqVal> seqVals = selectData("seq_portion", sql, ResSeqVal.class, Collections.<SqlParam>emptyList());
		ArrayList<Long> result = new ArrayList<Long>();
		for (ResSeqVal item : seqVals) {
			result.add(item.seqval);
		}
		return result;
	}

	// use 1 call for postgres, instead of 2 (sequence and insert)
	protected long createTaskInternal(String name, String channel, String data, String key1, String key2, Long batchId, int postponeSec, TedStatus status) {
		String sqlLogId = "create_task";
		String sql = " insert into tedtask (taskId, system, name, channel, bno, status, createTs, nextTs, retries, data, key1, key2, batchId)" +
				" values($nextTaskId, '$sys', ?, ?, null, '$status', $now, $now + $postpone, 0, ?, ?, ?, ?)" +
				" returning taskId";
		sql = sql.replace("$nextTaskId", dbType.sql.sequenceSql("SEQ_TEDTASK_ID"));
		sql = sql.replace("$now", dbType.sql.now());
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$postpone", dbType.sql.intervalSeconds(postponeSec));
		sql = sql.replace("$status", (status == null ? TedStatus.NEW : status).toString());

		List<TaskIdRes> resList = selectData(sqlLogId, sql, TaskIdRes.class, asList(
				sqlParam(name, JetJdbcParamType.STRING),
				sqlParam(channel, JetJdbcParamType.STRING),
				sqlParam(data, JetJdbcParamType.STRING),
				sqlParam(key1, JetJdbcParamType.STRING),
				sqlParam(key2, JetJdbcParamType.STRING),
				sqlParam(batchId, JetJdbcParamType.LONG)
		));
		Long taskId = resList.get(0).taskid;
		logger.trace("Task {} {} created successfully. ", name, taskId);
		return taskId;

	}


}
