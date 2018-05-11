package com.github.ted.sys;

import com.github.ted.Ted.TedStatus;
import com.github.ted.sys.JdbcSelectTed.SqlParam;
import com.github.ted.sys.Model.TaskParam;
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

/**
 * @author Augustus
 *         created on 2017.04.14
 *
 * for TED internal usage only!!!
 *
 */
class TedDaoPostgres extends TedDaoAbstract {
	private static final Logger logger = LoggerFactory.getLogger(TedDaoPostgres.class);

	public TedDaoPostgres(String system, DataSource dataSource) {
		super(system, dataSource, DbType.POSTGRES);
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
}
