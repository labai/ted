package ted.scheduler.impl

import ted.scheduler.impl.JdbcSelectTed.JetJdbcParamType
import ted.scheduler.impl.JdbcSelectTed.SqlParam
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
internal object SchdJdbcSelect {

    internal fun sqlParam(value: Any, type: JetJdbcParamType): SqlParam {
        return SqlParam(null, value, type)
    }

    internal fun <T> selectData(dataSource: DataSource, sql: String, clazz: Class<T>, sqlParams: List<SqlParam>): List<T> {
        return JdbcSelectTed.runInConn(dataSource) { connection ->
            JdbcSelectTedImpl.selectData(connection, sql, clazz, sqlParams)
        }
    }

    internal fun executeUpdate(dataSource: DataSource, sql: String, sqlParams: List<SqlParam>): Int {
        return JdbcSelectTed.runInConn(dataSource) { connection ->
            JdbcSelectTedImpl.executeUpdate(connection, sql, sqlParams)
        }
    }

    @Throws(SQLException::class)
    internal fun <T> selectData(connection: Connection, sql: String, clazz: Class<T>, sqlParams: List<SqlParam>): List<T> {
        return JdbcSelectTedImpl.selectData(connection, sql, clazz, sqlParams)
    }
}
