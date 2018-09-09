package ted.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.sys._TedSchdJdbcSelect;
import ted.driver.sys._TedSchdJdbcSelect.JetJdbcParamType;
import ted.driver.sys._TedSchdJdbcSelect.SqlParam;
import ted.driver.sys._TedSchdJdbcSelect.TedSqlException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static ted.driver.sys._TedSchdJdbcSelect.sqlParam;


/**
 * @author Augustus
 *         created on 2018.08.16
 *
 *  for TED internal usage only!!!
 *
 *  Dao functions for ted-scheduler (PostgreSql)
 *
 */
class DaoPostgres {
	private static final Logger logger = LoggerFactory.getLogger(DaoPostgres.class);
	private final DataSource dataSource;
	private final String thisSystem;

	DaoPostgres(DataSource dataSource, String systemId) {
		this.dataSource = dataSource;
		this.thisSystem = systemId;
	}

	static class TxContext {
		private boolean rollbacked = false;
		public final Connection connection;
		public TxContext(Connection connection) {
			this.connection = connection;
		}
		public void rollback() {
			try {
				connection.rollback();
			} catch (SQLException e) {
				throw new TedSqlException("Cannot rollback", e);
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
			res = _TedSchdJdbcSelect.selectData(connection, sql, LongVal.class, Collections.emptyList());
		} catch (SQLException e) {
			return false;
		}
		if (res.isEmpty() || res.get(0).longVal == 0L)
			return false;
		return true;
	}


	// returned count: 0 - not exists, 1 - exists 1, 2 - exists more than 1
	// includingError - do include with status ERROR?
	// todo SLEEP?
	List<Long> get2ActiveTasks(String taskName, boolean includingError) {
		String sqlLogId = "chk_uniq_task";
		String sql = "select taskid as longVal from tedtask where system = '$sys' and name = ?"
				+ " and status in ('NEW', 'RETRY', 'WORK'"+ (includingError?",'ERROR'":"") +")"
				+ " limit 2";
		sql = sql.replace("$sys", thisSystem);
		List<LongVal> results = selectData(sqlLogId, sql, LongVal.class, asList(
				sqlParam(taskName, JetJdbcParamType.STRING)
		));
		return results.stream().map(longVal -> longVal.longVal).collect(Collectors.toList());
	}

	void restoreFromError(Long taskId, String taskName, int postponeSec) {
		String sqlLogId = "restore_from_error";
		String sql = "update tedtask set status = 'RETRY', retries = retries + 1, "
				+ " nextts = now() + interval '$postponeSec seconds' "
				+ " where system = '$sys' and taskid = ? and name = ?"
				+ " and status = 'ERROR'"
				+ " returning tedtask.taskid";
		sql = sql.replace("$sys", thisSystem);
		sql = sql.replace("$postponeSec", String.valueOf(postponeSec));

		selectData(sqlLogId, sql, LongVal.class, asList(
				sqlParam(taskId, JetJdbcParamType.LONG),
				sqlParam(taskName, JetJdbcParamType.STRING)
		));
	}

	List<Long> checkForErrorStatus(Collection<Long> taskIds) {
		String sqlLogId = "check_for_errors";
		if (taskIds.isEmpty())
			return Collections.emptyList();
		String inIds = taskIds.stream().map(String::valueOf).collect(Collectors.joining(","));
		String sql = "select taskid as longVal from tedtask"
				+ " where system = '$sys'"
				+ " and status = 'ERROR'"
				+ " and taskid in (" + inIds + ")";
		sql = sql.replace("$sys", thisSystem);

		List<LongVal> results = selectData(sqlLogId, sql, LongVal.class, Collections.emptyList());
		return results.stream().map(longVal -> longVal.longVal).collect(Collectors.toList());
	}


	protected <T> List<T> selectData(String sqlLogId, String sql, Class<T> clazz, List<SqlParam> params) {
		long startTm = System.currentTimeMillis();
		List<T> list = _TedSchdJdbcSelect.selectData(dataSource, sql, clazz, params);
		long durationMs = System.currentTimeMillis() - startTm;
		if (durationMs >= 50)
			logger.info("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size());
		else
			logger.debug("After [{}] time={}ms items={}", sqlLogId, durationMs, list.size());
		return list;
	}

}
