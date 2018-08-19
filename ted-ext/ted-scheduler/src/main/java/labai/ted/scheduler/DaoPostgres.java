package labai.ted.scheduler;

import labai.ted.scheduler.JdbcSelectTed.JetJdbcParamType;
import labai.ted.scheduler.JdbcSelectTed.SqlParam;
import labai.ted.scheduler.JdbcSelectTed.TedSqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static labai.ted.scheduler.JdbcSelectTed.sqlParam;

/**
 * @author Augustus
 *         created on 2018.08.16
 *
 *  for TED internal usage only!!!
 *
 *  Dao functions for ted-scheduler (PostgreSql)
 *
 */
public class DaoPostgres {
	private static final Logger logger = LoggerFactory.getLogger(DaoPostgres.class);
	private final DataSource dataSource;
	private final String thisSystem;

	private Long primeTaskId = null;

	public DaoPostgres(DataSource dataSource, String systemId) {
		this.dataSource = dataSource;
		this.thisSystem = systemId;
	}

	public static class TxContext {
		private boolean rollbacked = false;
		public final Connection connection;
		public TxContext(Connection connection) {
			this.connection = connection;
		}
		public void rollback() {
			try {
				connection.rollback();
			} catch (SQLException e) {
				throw new RuntimeException("Cannot rollback", e);
			}
			rollbacked = true;
		};
	}
	<T> T execWithLockedPrimeTaskId(DataSource dataSource, Long primeTaskId, Function<TxContext, T> function) {
		if (primeTaskId == null) throw new IllegalStateException("primeTaskId is null");
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new TedSqlException("Cannot get DB connection", e);
		}
		try {
			return txRun(connection, tx -> {
				if (! advisoryLockPrimeTask(tx.connection, primeTaskId)) {
					logger.debug("Cannot get advisoryLockPrimeTask for primeTaskId={}, skipping", primeTaskId);
					return null;
				}
				return function.apply(tx);
			});

		} finally {
			try { if (connection != null) connection.close(); } catch (Exception e) {logger.error("Cannot close connection", e);};
		}
	}

	<T> T txRun(Connection connection, Function<TxContext, T> function) {
		Savepoint savepoint = null;
		Boolean autocommit = null;
		// TODO autocommit?
		try {
			autocommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			savepoint = connection.setSavepoint("txRun");
			String txLogId = Integer.toHexString(savepoint.hashCode());
			logger.debug("[B] before start transaction {}", txLogId);
			//em.getTransaction().begin();
			logger.trace("[B] after start transaction {}", txLogId);
			TxContext txContext = new TxContext(connection);
			T result = function.apply(txContext);
			if (! txContext.rollbacked) {
				logger.trace("[E] before commit transaction {}", txLogId);
				connection.commit();
				logger.debug("[E] after commit transaction {}", txLogId);
			}
			return result;
		} catch (Throwable e) {
			//if (!session.getTransaction().wasCommitted() && !session.getTransaction().wasRolledBack()) {
			try {
				if (savepoint != null)
					connection.rollback(savepoint);
				else
					connection.rollback();
			} catch (Exception rollbackException) {
				e.addSuppressed(rollbackException);
			}

			//}
			throw new RuntimeException(e);
		}
		finally {
			try {
				if (autocommit != null && autocommit != connection.getAutoCommit()) {
					connection.setAutoCommit(autocommit);
				}
			} catch (SQLException e) {
				logger.warn("Exception while setting back autoCommit mode", e);
			}
//			if (connection.isActive()) {
//				logger.warn("Transaction was still active, rollbacking");
//				em.getTransaction().rollback();
//			}
//			try { em.close(); } catch (Exception e) { logger.warn("Cannot close entityManager", e); }

		}
	}

	private static class LongVal {
		private Long longVal;
	}

	// use advisory lock in Postgres mechanism
	private boolean advisoryLockPrimeTask(Connection connection, long primeTaskId) {
		logger.debug("advisory lock {}", primeTaskId);
		String sql = "select case when pg_try_advisory_xact_lock(" + 1977110801 + ", " + primeTaskId + ") then 1 else 0 end as longVal";
		List<LongVal> res;
		try {
			res = JdbcSelectTed.selectData(connection, sql, LongVal.class, Collections.emptyList());
		} catch (SQLException e) {
			return false;
		}
		if (res.isEmpty() || res.get(0).longVal == 0L)
			return false;
		return true;
	}

	boolean existsActiveTask(String taskName) {
		String sqlLogId = "chk_uniq_task";
		String sql = "select taskid as longVal from tedtask where system = '$sys' and name = ?"
				+ " and status in ('NEW', 'RETRY', 'WORK')"
				+ " limit 1";
		sql = sql.replace("$sys", thisSystem);
		List<LongVal> results = selectData(sqlLogId, sql, LongVal.class, asList(
				sqlParam(taskName, JetJdbcParamType.STRING)
		));
		return results.size() > 0;
	}

	protected <T> List<T> selectData(String sqlLogId, String sql, Class<T> clazz, List<SqlParam> params) {
		logSqlParams(sqlLogId, sql, params);
		long startTm = System.currentTimeMillis();
		List<T> list = Collections.emptyList();
		try {
			list = JdbcSelectTed.selectData(dataSource, sql, clazz, params);
		} catch (SQLException sqle) {
			logger.error("SQLException while execute '{}': {}. SQL={}", sqlLogId, sqle.getMessage(), sql);
			throw new JdbcSelectTed.TedSqlException("SQL exception while calling sqlId '" + sqlLogId + "'", sqle);
		}
		long durationMs = System.currentTimeMillis() - startTm;
		if (durationMs >= 50)
			logger.info("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size());
		else
			logger.debug("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size());
		return list;
	}

	private void logSqlParams(String sqlLogId, String sql, List<SqlParam> params) {
		if (logger.isTraceEnabled()) {
			String sparams = "";
			for (SqlParam p : params)
				sparams += String.format(" %s=%s", p.code, p.value);
			logger.trace("Before[{}] with params:{}", sqlLogId, sparams);
			if (logger.isTraceEnabled()) {
				logger.trace("sql:" + sql);
			}
		}
	}
}
