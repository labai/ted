package com.github.labai.ted.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	public static <T> List<T> executeBlock(Connection connection, String sql, Class<T> clazz, List<SqlParam> sqlParams) throws SQLException {
		return JdbcSelectTedImpl.executeOraBlock(connection, sql, clazz, sqlParams);
	}

	public static <T> List<T> selectData(Connection connection, String sql, Class<T> clazz, List<SqlParam> sqlParams) throws SQLException {
		return JdbcSelectTedImpl.selectData(connection, sql, clazz, sqlParams);
	}

	public static void execute(Connection connection, String sql, List<SqlParam> sqlParams) throws SQLException {
		JdbcSelectTedImpl.execute(connection, sql, sqlParams);
	}

	public static Long selectSingleLong(Connection connection, String sql, List<SqlParam> sqlParams) throws SQLException {
		return JdbcSelectTedImpl.selectSingleLong(connection, sql, sqlParams);
	}

}

