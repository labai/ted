package ted.driver.sys;

import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.JdbcSelectTed.SqlParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2015-03-23 (SqlSelect)
 *
 *         changed on 2016-09-12
 *
 * for TED internal usage only!!!
 *
 */
class JdbcSelectTedImpl {
	private static final Logger logger = LoggerFactory.getLogger(JdbcSelectTedImpl.class);

	private final static boolean oracleDriverExists;

	static {
		boolean exists;
		try {
			Class.forName("oracle.sql.BLOB");
			Class.forName("oracle.sql.TIMESTAMP");
			exists = true;
		} catch (ClassNotFoundException e) {
			exists = false;
		}
		oracleDriverExists = exists;
	}

	// equals to one in hibernate
	private interface ResultTransformer extends Serializable {
		Object transformTuple(Object[] var1, String[] var2);
		List transformList(List var1);
	}

	/**
	 */
	private static class AliasToPojoResultTransformer<T> implements ResultTransformer {

		private final Class<T> clazz;
		private final Constructor<T> noArgConstructor;
		private ArrayList<Field> entityFields = null;

		public AliasToPojoResultTransformer(Class<T> resultClass) {
			if (resultClass == null) throw new IllegalArgumentException("resultClass cannot be null");
			this.clazz = resultClass;
			this.noArgConstructor = getNoArgConstructor(this.clazz);
		}

		private static <T> Constructor<T> getNoArgConstructor(Class<T> clazz) {
			for (Constructor c: clazz.getDeclaredConstructors()) {
				if (c.getParameterTypes().length == 0) {
					c.setAccessible(true);
					return c;
				}
			}
			throw new RuntimeException("Constructor w/o args was not found for class '" + clazz.getCanonicalName() + "'");
		}

		private static <T> T arrayToClass(Object dataArray, Constructor<T> noArgConstructor, ArrayList<Field> entityFields) throws SQLDataException {
			T object;
			try {
				object = noArgConstructor.newInstance();
			} catch (InstantiationException e) {
				throw new SQLDataException("Cannot create bean instance", e);
			} catch (IllegalAccessException e) {
				throw new SQLDataException("Cannot access bean", e);
			} catch (InvocationTargetException e) {
				throw new SQLDataException("Cannot create bean instance", e);
			}
			copyArrayToBean(dataArray, object, entityFields);
			return object;
		}

		private static boolean matches(String name1, String name2) {
			if (name1 == null || name2 == null) return false;
			//if (name1.equalsIgnoreCase(name2)) return true;
			return name1.replaceAll("_", "").equalsIgnoreCase(name2.replaceAll("_", ""));
		}

		public T transformTuple(Object[] tuple, String[] aliases) {
			T result;
			try {
				if (entityFields == null) {
					entityFields = new ArrayList<Field>(asList(new Field[aliases.length]));

					for (Field field : clazz.getDeclaredFields()) {
						field.setAccessible(true);
						String name = field.getName();
						for (int i = 0; i < aliases.length; i++) {
							if (entityFields.get(i) != null) continue;
							if (matches(name, aliases[i])) {
								entityFields.set(i, field);
							}
						}
					}
				}
				result = arrayToClass(tuple, noArgConstructor, entityFields);

			} catch (SQLDataException e) {
				throw new RuntimeException("An error occurred while transforming response", e);
			}

			return result;
		}

		public List transformList(List collection) {
			return collection;
		}

	}


	/**
	 * return ResultTransformer for hibernate template.execute.
	 * Transformer will convert resultSet to Java class.
	 * DB fields can be any case, with underscore or not
	 * (field / column match is forgiving).
	 */
	private static <T> ResultTransformer resultTransformer(Class<T> clazz){
		return new AliasToPojoResultTransformer<T>(clazz);
	}

	static <T> List<T> resultSetToList(ResultSet resultSet, Class<T> clazz) throws SQLException {
		List<T> list = new ArrayList<T>();
		if (resultSet == null)
			return list;
		ResultSetMetaData rsmd = resultSet.getMetaData();
		String[] cols = null;
		AliasToPojoResultTransformer<T> transformer = new AliasToPojoResultTransformer<T>(clazz);
		Object[] values = new Object[rsmd.getColumnCount()];

		while (resultSet.next()){
			if (cols == null) { // on first
				cols = new String[rsmd.getColumnCount()];
				for (int i = 1; i <= rsmd.getColumnCount(); i++){
					cols[i - 1] = rsmd.getColumnName(i);
				}
			}
			for (int icol = 1; icol <= rsmd.getColumnCount(); icol++){
				values[icol - 1] = resultSet.getObject(icol);
			}
			T bean = transformer.transformTuple(values, cols);
			list.add(bean);
		}
		resultSet.close();
		return list;
	}

	private static void copyArrayToBean(Object dataRow, Object bean, ArrayList<Field> entityFields) throws SQLDataException {
		Class<?> clazz = bean.getClass();
		Object[] dataArray = null;
		Object dataObject = null;
		if (dataRow instanceof Object[]) {
			dataArray = (Object[]) dataRow;
		} else {
			dataObject = dataRow;
		}
		if (entityFields.size() != 1 && dataObject != null)
			throw new SQLDataException("entityFields.size != 1, but single column was returned");

		for (int iPos = 0; iPos < entityFields.size(); iPos++){
			Field fld = entityFields.get(iPos);
			if (fld == null) continue;
			Object colData = dataObject != null ? dataObject : dataArray[iPos];
			try {
				Class<?> type = fld.getType();
				if (colData == null)
					fld.set(bean, null);
				else if (type.isAssignableFrom(Long.class))
					fld.set(bean, getLong(colData));
				else if (type.isAssignableFrom(Integer.class))
					fld.set(bean, getLong(colData).intValue());
				else if (type.isAssignableFrom(BigDecimal.class))
					fld.set(bean, getBigDecimal(colData));
				else if (type.isAssignableFrom(String.class)) {
					fld.set(bean, colData.toString());
				} else if (type.getSimpleName().equals("byte[]")) {
					if (trySetOracleBLOB(fld, bean, colData)) {
						// done..
					}
				} else if (type.isAssignableFrom(Date.class)) {
					if (trySetOracleTIMESTAMP(fld, bean, colData)) {
						// done..
					} else if (colData instanceof Date) {
						fld.set(bean, new Date(((Date)colData).getTime())); // will lose nanos, tmz?
					} else {
						throw new SQLDataException("Cannot assign Date");
					}
				} else if (type.isAssignableFrom(Boolean.class) || type == boolean.class)
					fld.set(bean, "Y".equalsIgnoreCase(colData.toString()) || "1".equals(colData.toString()));
				//else fld.set(bean, colData);
			} catch (IllegalArgumentException e) {
				throw new SQLDataException("Cannot assign bean property", e);
			} catch (IllegalAccessException e) {
				throw new SQLDataException("Cannot access bean property", e);
			} catch (SQLException e) {
				throw new SQLDataException("Cannot access bean property", e);
			}
		}
		return;
	}

	private static boolean trySetOracleTIMESTAMP(Field fld, Object bean, Object colData) throws IllegalAccessException, SQLDataException {
		if (!oracleDriverExists)
			return false;
		if (colData instanceof oracle.sql.TIMESTAMP) {
			try {
				fld.set(bean, ((oracle.sql.TIMESTAMP) colData).dateValue());
				return true;
			} catch (SQLException e) {
				throw new SQLDataException("Cannot read from oracle.sql.TIMESTAMP", e);
			}
		}
		return false;
	}

	private static boolean trySetOracleBLOB(Field fld, Object bean, Object colData) throws IllegalAccessException, SQLException {
		if (!oracleDriverExists)
			return false;
		if (colData instanceof oracle.sql.BLOB) {
			oracle.sql.BLOB blob = ((oracle.sql.BLOB)colData);
			fld.set(bean, blob.getBytes(1, (int) blob.length()));
			blob.free(); //release the blob and free up memory.
			return true;
		}
		return false;
	}

	/**
	 * 	Execute sql block or stored procedure.
	 * 	If exists output parameter with type CURSOR, then return resultSet as List of clazz type objects.
	 */
	static <T> List<T> executeOraBlock(Connection connection, String sql, Class<T> clazz, List<SqlParam> sqlParams) throws SQLException {
		String cursorParam = null;
		List<T> list = new ArrayList<T>();
		//boolean autoCommitOrig = connection.getAutoCommit();
		//if (autoCommitOrig == true) {
		//	connection.setAutoCommit(false); // to able to read from temp-tables
		//}

		CallableStatement stmt = null;
		ResultSet resultSet = null;
		try {
			stmt = connection.prepareCall(sql);
			cursorParam = stmtAssignSqlParams(stmt, sqlParams);
			boolean hasRs = stmt.execute();
			if (cursorParam != null) {
				resultSet = (ResultSet) stmt.getObject(cursorParam);
				list = resultSetToList(resultSet, clazz);
			}
		} finally {
			try { if (resultSet != null) resultSet.close(); } catch (Exception e) {logger.error("Cannot close resultSet", e);};
			//if (autoCommitOrig == true)
			//	connection.setAutoCommit(autoCommitOrig);
			try { if (stmt != null) stmt.close(); } catch (Exception e) {logger.error("Cannot close statement", e);};
		}
		return list;
	}

	static <T> List<T> selectData(Connection connection, String sql, Class<T> clazz, List<SqlParam> sqlParams) throws SQLException {
		List<T> list;
		CallableStatement stmt = null;
		ResultSet resultSet = null;
		try {
			stmt = connection.prepareCall(sql);
			stmtAssignSqlParams(stmt, sqlParams);
			resultSet = stmt.executeQuery();
			list = resultSetToList(resultSet, clazz);
		} finally {
			try { if (resultSet != null) resultSet.close(); } catch (Exception e) {logger.error("Cannot close resultSet", e);};
			try { if (stmt != null) stmt.close(); } catch (Exception e) {logger.error("Cannot close statement", e);};
		}
		return list;
	}


	static int executeUpdate(Connection connection, String sql, List<SqlParam> sqlParams) throws SQLException {
		CallableStatement stmt = null;
		int res = 0;
		try {
			stmt = connection.prepareCall(sql);
			stmtAssignSqlParams(stmt, sqlParams);
			res = stmt.executeUpdate();
		} finally {
			try { if (stmt != null) stmt.close(); } catch (Exception e) {logger.error("Cannot close statement", e);};
		}
		return res;
	}

	/**
	 * 	Execute sql.
	 */
//	static void execute(Connection connection, String sql, List<SqlParam> sqlParams) throws SQLException {
//		CallableStatement stmt = null;
//		try {
//			stmt = connection.prepareCall(sql);
//			stmtAssignSqlParams(stmt, sqlParams);
//			boolean hasRs = stmt.execute();
//		} finally {
//			try { if (stmt != null) stmt.close(); } catch (Exception e) {logger.error("Cannot close statement", e);};
//		}
//		return;
//	}

	private static BigDecimal findDecimalSingleValue(Connection connection, String sql, List<SqlParam> sqlParams) throws SQLException {
		CallableStatement stmt = null;
		ResultSet resultSet = null;
		try {
			stmt = connection.prepareCall(sql);
			stmtAssignSqlParams(stmt, sqlParams);
			resultSet = stmt.executeQuery();
			if (resultSet == null /*|| resultSet.isClosed()*/) // isClosed may throw AbstractMethod error for older versions of driver
				throw new SQLException("ResultSet is null or closed");

			ResultSetMetaData rsmd = resultSet.getMetaData();
			if (rsmd.getColumnCount() != 1)
				throw new SQLException("ResultSet must have exactly 1 column");
			if (!resultSet.next())
				throw new SQLException("ResultSet must have exactly 1 row");
			Object object = resultSet.getObject(1);
			if (resultSet.next())
				throw new SQLException("ResultSet have more than 1 row");
			if (object == null)
				return null;

			return getBigDecimal(object);

		} finally {
			try { if (resultSet != null) resultSet.close(); } catch (Exception e) {logger.error("Cannot close resultSet", e);};
			try { if (stmt != null) stmt.close(); } catch (Exception e) {logger.error("Cannot close statement", e);};
		}
	}

	private static Long getLong(Object object) {
		if (object == null)
			return null;
		if (Long.class.isAssignableFrom(object.getClass()))
			return (Long)object;
		if (Integer.class.isAssignableFrom(object.getClass()))
			return (long)(Integer)object;
		if (object.getClass().equals(BigDecimal.class))
			return ((BigDecimal) object).longValue();
		if (Number.class.isAssignableFrom(object.getClass()))
			return new BigDecimal(object.toString()).longValue();
		throw new RuntimeException("Cannot cast to Long object of type: " + object.getClass().getName());
	}

	private static BigDecimal getBigDecimal(Object object) {
		if (object == null)
			return null;
		if (object.getClass().equals(BigDecimal.class))
			return (BigDecimal) object;
		if (Number.class.isAssignableFrom(object.getClass()))
			return new BigDecimal(object.toString());
		throw new RuntimeException("Cannot cast to BigDecimal object of type: " + object.getClass().getName());
	}

	private static BigDecimal selectSingleDecimal(Connection connection, String sql, List<SqlParam> sqlParams) throws SQLException {
		return findDecimalSingleValue(connection, sql, sqlParams);
	}

	public static Long selectSingleLong(Connection connection, String sql, List<SqlParam> sqlParams) throws SQLException {
		BigDecimal res = selectSingleDecimal(connection, sql, sqlParams);
		return res == null ? null : res.longValue();
	}

	// assigns parameters
	// support input STRING, INTEGER, LONG, TIMESTAMP, DATE, BLOB
	// returns name of CURSOR parameter
	// named parameters are not supported in Postgres
	static String stmtAssignSqlParams(CallableStatement stmt, List<SqlParam> sqlParams) throws SQLException {
		String cursorParam = null;
		if (sqlParams == null)
			return null;

		int pos = 0;
		for (SqlParam param : sqlParams) {
			pos++;
			if (param.type == null)
				throw new SQLDataException("Please provide parameter type for param.code='" + param.code + "' pos=" + pos);

			switch (param.type) {
				case CURSOR:
					cursorParam = param.code;
					if (param.code == null)
						stmt.registerOutParameter(pos, JetJdbcParamType.CURSOR.oracleId);
					else
						stmt.registerOutParameter(param.code, JetJdbcParamType.CURSOR.oracleId);
					break;
				case STRING:
					if (param.code == null)
						stmt.setString(pos, param.value == null ? null : param.value.toString());
					else
						stmt.setString(param.code, param.value == null ? null : param.value.toString());
					break;
				case INTEGER: case LONG:
					if (param.value == null) {
						if (param.code == null)
							stmt.setNull(pos, Types.BIGINT);
						else
							stmt.setNull(param.code, Types.BIGINT);
					} else {
						long val = 0;
						if (param.value.getClass().isAssignableFrom(Integer.class)) {
							val = (Integer) param.value;
						} else {
							val = (Long) param.value;
						}
						if (param.code == null)
							stmt.setLong(pos, val);
						else
							stmt.setLong(param.code, val);
					}
					break;
				case TIMESTAMP:
					Date date1 = (Date) param.value;
					if (param.code == null)
						stmt.setTimestamp(pos, param.value == null ? null : new Timestamp(date1.getTime()));
					else
						stmt.setTimestamp(param.code, param.value == null ? null : new Timestamp(date1.getTime()));
					break;
				case DATE:
					Date date2 = (Date) param.value;
					if (param.code == null)
						stmt.setDate(pos, param.value == null ? null : new java.sql.Date(date2.getTime()));
					else
						stmt.setDate(param.code, param.value == null ? null : new java.sql.Date(date2.getTime()));
					break;
				case BLOB:
					if (param.value == null) {
						if (param.code == null)
							stmt.setBlob(pos, new ByteArrayInputStream(new byte[0]));
						else
							stmt.setBlob(param.code, new ByteArrayInputStream(new byte[0]));
					} else {
						byte[] bytes = (byte[]) param.value;
						if (param.code == null)
							stmt.setBlob(pos, new ByteArrayInputStream(bytes), (int) bytes.length);
						else
							stmt.setBlob(param.code, new ByteArrayInputStream(bytes), (int) bytes.length);
					}
					break;
				default:
					throw new IllegalStateException("Data type '" + param.type.toString() + "' is not supported yet");
			}
		}
		return cursorParam;
	}

}
