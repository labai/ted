package labai.ted.scheduler;

import labai.ted.Ted.TedDbType;

import javax.sql.DataSource;

/**
 * @author Augustus
 *         created on 2016.09.22
 */
class TestConfig {
	static final boolean INT_TESTS_ENABLED = true;
	static final String SYSTEM_ID = "ted.test";
	static final TedDbType testDbType = TedDbType.POSTGRES; // which one we are testing


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

	static DataSource getDataSource() {
		if (testDbType == TedDbType.POSTGRES)
			return TestUtils.dbConnectionProviderPostgres();
		throw new IllegalStateException("Invalid dbType:" + testDbType);
	}

}
