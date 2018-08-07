package com.github.labai.ted.sys;

import com.github.labai.ted.sys.JdbcSelectTed.JetJdbcParamType;
import com.github.labai.ted.sys.JdbcSelectTed.SqlParam;
import com.github.labai.ted.sys.Model.TaskParam;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.PrimeInstance.CheckPrimeParams;
import com.github.labai.ted.sys.QuickCheck.CheckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.labai.ted.sys.JdbcSelectTed.sqlParam;
import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 *
 * rewrite functions for faster execution using Oracle feature - block statements
 *
 */
class TedDaoOracle extends TedDaoAbstract {
	private static final Logger logger = LoggerFactory.getLogger(TedDaoAbstract.class);

	public TedDaoOracle(String system, DataSource dataSource) {
		super(system, dataSource, DbType.ORACLE);
	}

	@Override
	public List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes){
		String channelsParam = "";
		for (String channel : channelSizes.keySet()) {
			int cnt = channelSizes.get(channel);
			if (cnt == 0) continue;
			channelsParam = ("".equals(channelsParam) ? "" : channelsParam + ",") + channel + ":" + cnt;
		}

		String oraProc = "get_next_portion_all(or)";
		String sql = ""
				+ " declare"
				+ "   v_bno number;"
				+ "   now timestamp := systimestamp;"
				+ "	  p_sys tedtask.system%type := :p_sys;"
				+ "   p_pairs varchar(500) := :p_pairs; "
				+ "   v_taskid number;"

				+ "   cursor c2 (vchan in varchar, vcnt in integer) is"
				+ "     select taskid from tedtask"
				+ "     where status in ('NEW', 'RETRY') and system = p_sys and channel = vchan"
				+ "       and nextTs < now "
				+ "       and rownum <= vcnt"
				+ "       for update skip locked;"

				+ " begin"
				+ 	" v_bno := SEQ_TEDTASK_BNO.nextval;"

				// iterate through 'channel:count,channel2:count2' pairs
				+ 	" for cur in (select substr(str, 1, instr(str, ':') - 1) chan, to_number(substr(str, instr(str, ':') + 1)) cnt"
				+ 	"    	from (select trim(regexp_substr(p_pairs, '[^,]+', 1, level)) str from dual connect by instr(p_pairs, ',', 1, level - 1) > 0)"
				+ 		" 		) loop"

				// take NEW and RETRY tasks of channel cur.chan, maximum cur.cnt
				+		" open c2(cur.chan, cur.cnt); "
				+ 		" loop "
				+ 		" 	fetch c2 into v_taskid; "
				+ 		" 	exit when c2%notfound; "
				+		"   update tedtask set status = 'WORK', bno = v_bno, startTs = now, nextTs = null"
				+ 		"      where current of c2;"
				+ 		" end loop;"
				+ 		" close c2;"

				+	" end loop;"
				+ 	" commit;"

				// return all taken tasks
				+ 	" open :o_rs for select * from tedtask where bno = v_bno;"
				+ " end;";
		List<TaskRec> tasks = selectFromBlock(oraProc, sql, TaskRec.class, asList(
				sqlParam("p_sys", thisSystem, JetJdbcParamType.STRING),
				sqlParam("p_pairs", channelsParam, JetJdbcParamType.STRING),
				sqlParam("o_rs", "", JetJdbcParamType.CURSOR)
		));
		return tasks;
	}

	@Override
	public List<CheckResult> quickCheck(CheckPrimeParams checkPrimeParams) {
		if (checkPrimeParams != null)
			throw new IllegalStateException("TODO for oracle");

		List<String> chans = getWaitChannels();
		List<CheckResult> res = new ArrayList<CheckResult>();
		for (String chan : chans) {
			res.add(new CheckResult("CHAN", chan));
		}
		return res;
	}

	// TODO is not really bulk. Do we care about Oracle yet?
	@Override
	public List<Long> createTasksBulk(List<TaskParam> taskParams) {
		ArrayList<Long> taskIds = new ArrayList<Long>();
		for (TaskParam param : taskParams) {
			Long taskId = createTask(param.name, param.channel, param.data, param.key1, param.key2, param.batchId);
			taskIds.add(taskId);
		}
		return taskIds;
	}

	@Override
	public boolean becomePrime(Long primeTaskId, String instanceId) {
		throw new IllegalStateException("TODO for oracle");
	}

	@Override
	public Long findPrimeTaskId() {
		throw new IllegalStateException("TODO for oracle");
	}

	@Override
	public Long createEvent(String taskName, String discriminator, String data, String key2) {
		throw new IllegalStateException("TODO for oracle");
	}

	@Override
	public TaskRec eventQueueMakeFirst(String discriminator) {
		throw new IllegalStateException("TODO for oracle");
	}

	@Override
	public List<TaskRec> eventQueueGetTail(String discriminator) {
		throw new IllegalStateException("TODO for oracle");
	}

	//
	// wrapper
	//
	<T> List<T> selectFromBlock(String sqlLogId, String sql, Class<T> clazz, List<SqlParam> params) {
		//String sql = makeCallProcSql(PACKAGE_NAME + "." + sql, params.size());
		if (logger.isTraceEnabled()) {
			String sparams = "";
			for (SqlParam p : params)
				sparams += String.format(" %s=%s", p.code, p.value);
			logger.trace("Before[{}] with params: {}", sqlLogId, sparams);
			if (logger.isTraceEnabled()) {
				logger.trace("BlockSql:" + sql);
			}
		}

		long startTm = System.currentTimeMillis();
		List<T> list = null;
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.debug("Failed to get DB connection: " + e.getMessage());
			throw new RuntimeException("Cannot get DB connection", e);
		}
		try {
			list = JdbcSelectTed.executeBlock(connection, sql, clazz, params);
		} catch (SQLException e) {
			logger.error("SQLException while selectSingleLong '{}': {}. SQL={}", sqlLogId, e.getMessage(), sql);
			throw new RuntimeException("SQL exception while calling sqlId '" + sqlLogId + "'", e);
		}

		logger.debug("After [{}] time={}ms items={}", sqlLogId, (System.currentTimeMillis() - startTm), list.size());
		return list;
	}

}
