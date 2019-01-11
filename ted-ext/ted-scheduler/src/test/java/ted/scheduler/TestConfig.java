package ted.scheduler;

import com.zaxxer.hikari.HikariDataSource;
import ted.driver.Ted.TedDbType;

import javax.sql.DataSource;

/**
 * @author Augustus
 *         created on 2016.09.22
 */
class TestConfig {
	static final boolean INT_TESTS_ENABLED = true;
	static final String SYSTEM_ID = "ted.test";
	static final TedDbType testDbType = TedDbType.POSTGRES; // which one we are testing

	private static DataSource dataSource = null;

	static DataSource getDataSource() {
		synchronized (TestConfig.class) {
			if (dataSource == null)
				dataSource = dataSource();
		}
		return dataSource;
	}

	private static DataSource dataSource() {
		HikariDataSource dataSource = new HikariDataSource();
		dataSource.setDriverClassName("org.postgresql.Driver");
		dataSource.setJdbcUrl("jdbc:postgresql://localhost:5433/ted");
		dataSource.setUsername("ted");
		dataSource.setPassword("ted");
		return dataSource;
	}

}
