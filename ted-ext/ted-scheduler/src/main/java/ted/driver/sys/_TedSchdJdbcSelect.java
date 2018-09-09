package ted.driver.sys;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Augustus
 *         created on 2018-08-20
 *
 * for TED internal usage only!!!
 *
 * reuse ted-driver JdbcSelectTed - for scheduler only!
 */
public class _TedSchdJdbcSelect {
	public enum JetJdbcParamType {
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

	public static class SqlParam extends JdbcSelectTed.SqlParam {
		public SqlParam(String code, Object value, JetJdbcParamType type) {
			super(code, value, (type == null ? null : JdbcSelectTed.JetJdbcParamType.valueOf(type.toString())));
		}
		public String code() { return code; }
		public Object value() { return value; }
	}
	public static class TedSqlException extends JdbcSelectTed.TedSqlException {
		public TedSqlException(String message, SQLException cause) {
			super(message, cause);
		}
	}

	public static SqlParam sqlParam(Object value, JetJdbcParamType type) {
		return new SqlParam(null, value, type);
	}

	public static <T> List<T> selectData(DataSource dataSource, String sql, Class<T> clazz, List<? extends JdbcSelectTed.SqlParam> sqlParams) {
		return JdbcSelectTed.runInConn(dataSource, connection -> JdbcSelectTedImpl.selectData(connection, sql, clazz, (List<JdbcSelectTed.SqlParam>) sqlParams));
	}
	public static <T> List<T> selectData(Connection connection, String sql, Class<T> clazz, List<? extends JdbcSelectTed.SqlParam> sqlParams) throws SQLException {
		return JdbcSelectTedImpl.selectData(connection, sql, clazz, (List<JdbcSelectTed.SqlParam>) sqlParams);
	}
}
