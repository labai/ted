package ted.scheduler

import com.zaxxer.hikari.HikariDataSource
import ted.driver.Ted.TedDbType

import javax.sql.DataSource

/**
 * @author Augustus
 * created on 2016.09.22
 */
internal object TestConfig {
    val INT_TESTS_ENABLED = true
    val SYSTEM_ID = "ted.test"
    val testDbType = TedDbType.POSTGRES // which one we are testing

    private var dataSource: DataSource? = null

    fun getDataSource(): DataSource {
        synchronized(TestConfig::class.java) {
            if (dataSource == null)
                dataSource = dataSource()
        }
        return dataSource!!
    }

    private fun dataSource(): DataSource {
        val dataSource = HikariDataSource()
        dataSource.driverClassName = "org.postgresql.Driver"
        dataSource.jdbcUrl = "jdbc:postgresql://localhost:5433/ted"
        dataSource.username = "ted"
        dataSource.password = "ted"
        return dataSource
    }

}
