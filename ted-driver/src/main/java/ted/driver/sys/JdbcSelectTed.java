package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Augustus
 *         created on 2016-09-12
 *
 * for TED internal usage only!!!
 *
 *  helper class for selects (based on SqlSelect)
 *  uses simple jdbc (i.e. do not use hibernate or other framework)
 *
 *  Use for Oracle db, PostgreSql db
 *
 *	Api:
 *		- executeOraBlock
 *		- enum JetJdbcParamType - data types
 *		- sqlParam - create param for executeOraBlock
 *
 */
class JdbcSelectTed {
	private static final Logger logger = LoggerFactory.getLogger(JdbcSelectTed.class);

	enum JetJdbcParamType {
		CURSOR		(-10),      // oracle.jdbc.CURSOR
		STRING		(12),		// java.sql.Types.VARCHAR
		INTEGER		(4), 		// java.sql.Types.INTEGER
		LONG		(2), 		// java.sql.Types.NUMERIC
		TIMESTAMP	(93),		// java.sql.Types.TIMESTAMP
		DATE		(91),		// java.sql.Types.DATE
		BLOB		(2004); 	// java.sql.Types.BLOB
		final int oracleId;
		JetJdbcParamType(int oracleId) {
			this.oracleId = oracleId;
		}
	}

	static class TedSqlException extends RuntimeException {
		public TedSqlException(String message, SQLException cause) {
			super(message, cause);
		}
	}
	static class TedSqlDuplicateException extends TedSqlException {
		public TedSqlDuplicateException(String message, SQLException cause) {
			super(message, cause);
		}
	}

	static class SqlParam {
		final String code;
		final Object value;
		final JetJdbcParamType type;
		SqlParam(String code, Object value, JetJdbcParamType type) {
			this.code = code;
			this.value = value;
			this.type = type;
		}
	}

	static SqlParam sqlParam(String code, Object value, JetJdbcParamType type) {
		return new SqlParam(code, value, type);
	}
	static SqlParam sqlParam(Object value, JetJdbcParamType type) {
		return new SqlParam(null, value, type);
	}

	/* one of parameters (preferable first, as hibernate standard) can be output cursor */
    /* e.g.: call pkg_fcc_pmdata.get_payments_end(?, ?) */
    /* TODO WARNING sequence, not names, of parameters is important */
	public static <T> List<T> executeBlock(DataSource dataSource, String sql, Class<T> clazz, List<SqlParam> sqlParams) throws SQLException {
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new TedSqlException("Cannot get DB connection", e);
		}
		try {
			return JdbcSelectTedImpl.executeOraBlock(connection, sql, clazz, sqlParams);
		} finally {
			try { if (connection != null) connection.close(); } catch (Exception e) {logger.error("Cannot close connection", e);};
		}
	}

	public static <T> List<T> selectData(DataSource dataSource, String sql, Class<T> clazz, List<SqlParam> sqlParams) throws SQLException {
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new TedSqlException("Cannot get DB connection", e);
		}
		try {
			return JdbcSelectTedImpl.selectData(connection, sql, clazz, sqlParams);
		} finally {
			try { if (connection != null) connection.close(); } catch (Exception e) {logger.error("Cannot close connection", e);};
		}
	}

	// do not forget to close connection
	public static <T> List<T> selectData(Connection connection, String sql, Class<T> clazz, List<SqlParam> sqlParams) throws SQLException {
		return JdbcSelectTedImpl.selectData(connection, sql, clazz, sqlParams);
	}


	public static void execute(DataSource dataSource, String sql, List<SqlParam> sqlParams) throws SQLException {
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new TedSqlException("Cannot get DB connection", e);
		}
		try {
			JdbcSelectTedImpl.execute(connection, sql, sqlParams);
		} finally {
			try { if (connection != null) connection.close(); } catch (Exception e) {logger.error("Cannot close connection", e);};
		}
	}

	public static Long selectSingleLong(DataSource dataSource, String sql, List<SqlParam> sqlParams) throws SQLException {
		Connection connection;
		try {
			connection = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get DB connection: " + e.getMessage());
			throw new TedSqlException("Cannot get DB connection", e);
		}
		try {
			return JdbcSelectTedImpl.selectSingleLong(connection, sql, sqlParams);
		} finally {
			try { if (connection != null) connection.close(); } catch (Exception e) {logger.error("Cannot close connection", e);};
		}
	}

}

