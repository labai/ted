package ted.driver.sys;

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
	//static final TedDbType testDbType = TedDbType.ORACLE; // which one we are testing
	//static final TedDbType testDbType = TedDbType.MYSQL; // which one we are testing

	static class TedConnOracle {
		public final String URL;
		public final String USER;
		public final String PASSWORD;
		public TedConnOracle() {
			URL = "jdbc:oracle:thin:@localhost:1521:XE";
			USER = "ted";
			PASSWORD = "ted";
		}
	}

	static class TedConnPostgres {
		public final String URL;
		public final String USER;
		public final String PASSWORD;
		public TedConnPostgres() {
			URL = "jdbc:postgresql://localhost:5433/ted";
			USER = "ted";
			PASSWORD = "ted";
		}
	}

	static class TedConnMysql {
		public final String URL;
		public final String USER;
		public final String PASSWORD;
		public TedConnMysql() {
			URL = "jdbc:mysql://localhost:3308/ted";
			USER = "ted";
			PASSWORD = "ted";
		}
	}

	static DataSource getDataSource() {
		if (testDbType == TedDbType.ORACLE)
			return TestUtils.dbConnectionProviderOracle();
		if (testDbType == TedDbType.POSTGRES)
			return TestUtils.dbConnectionProviderPostgres();
		if (testDbType == TedDbType.MYSQL)
			return TestUtils.dbConnectionProviderMysql();
		throw new IllegalStateException("Invalid dbType:" + testDbType);
	}

}
