package ted.driver.sys

import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource

/**
 * @author Augustus
 * created on 2018-08-20
 *
 * for TED internal usage only!!!
 *
 * reuse ted-driver JdbcSelectTed - for scheduler only!
 */
object _TedSchdJdbcSelect {
    enum class JetJdbcParamType {
        CURSOR(JdbcSelectTed.JetJdbcParamType.CURSOR),
        STRING(JdbcSelectTed.JetJdbcParamType.STRING),
        INTEGER(JdbcSelectTed.JetJdbcParamType.INTEGER),
        LONG(JdbcSelectTed.JetJdbcParamType.LONG),
        TIMESTAMP(JdbcSelectTed.JetJdbcParamType.TIMESTAMP),
        DATE(JdbcSelectTed.JetJdbcParamType.DATE),
        BLOB(JdbcSelectTed.JetJdbcParamType.BLOB),;

        internal val tedType : JdbcSelectTed.JetJdbcParamType;

        constructor(tedType: JdbcSelectTed.JetJdbcParamType) {
            this.tedType = tedType
        }
    }

//    @PublishedApi
    internal class SqlParam(code: String?, value: Any, type: JetJdbcParamType?) :
            JdbcSelectTed.SqlParam(code, value, if (type == null) null else type.tedType) {
        fun code(): String = code
        fun value(): Any = value
    }

    internal class TedSqlException(message: String, cause: SQLException) :
            JdbcSelectTed.TedSqlException(message, cause)

    internal fun sqlParam(value: Any, type: JetJdbcParamType): SqlParam {
        return SqlParam(null, value, type)
    }

    internal fun <T> selectData(dataSource: DataSource, sql: String, clazz: Class<T>, sqlParams: List<JdbcSelectTed.SqlParam>): List<T> {
        return JdbcSelectTed.runInConn(dataSource, { connection -> JdbcSelectTedImpl.selectData(connection, sql, clazz, sqlParams) })
    }

    @Throws(SQLException::class)
    internal fun <T> selectData(connection: Connection, sql: String, clazz: Class<T>, sqlParams: List<JdbcSelectTed.SqlParam>): List<T> {
        return JdbcSelectTedImpl.selectData(connection, sql, clazz, sqlParams)
    }
}
