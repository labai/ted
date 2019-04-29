package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

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
		public TedSqlException(Throwable cause) {
			super(cause);
		}
		public TedSqlException(String message, SQLException cause) {
			super(message, cause);
		}
	}
	static class TedSqlDuplicateException extends TedSqlException {
		public TedSqlDuplicateException(String message, SQLException cause) {
			super(message, cause);
		}
	}

	interface ExecInConn<T> {
		T execute(Connection connection) throws SQLException;
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

	// ensure to close connection
	static <T> T runInConn(DataSource dataSource, ExecInConn<T> executor) {
		try (Connection connection = dataSource.getConnection()){
			return executor.execute(connection);
		} catch (SQLException e) {
			logger.error("Sql exception: " + e.getMessage());
			throw new TedSqlException(e);
		}
	}

}

