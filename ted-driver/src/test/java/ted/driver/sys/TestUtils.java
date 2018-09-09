package ted.driver.sys;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import ted.driver.sys.TestConfig.TedConnMysql;
import ted.driver.sys.TestConfig.TedConnOracle;
import ted.driver.sys.TestConfig.TedConnPostgres;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2016.09.20
 */
class TestUtils {

	private final static TedConnOracle tedConnOracle = new TedConnOracle();
	private final static TedConnPostgres tedConnPostgres = new TedConnPostgres();
	private final static TedConnMysql tedConnMysql = new TedConnMysql();

	private static SimpleDateFormat dateFormat = new SimpleDateFormat("mm:ss.SSS");
	//private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

	private static ComboPooledDataSource comboPooledDataSourceOracle = new ComboPooledDataSource();
	private static ComboPooledDataSource comboPooledDataSourcePostgres = new ComboPooledDataSource();
	private static ComboPooledDataSource comboPooledDataSourceMysql = new ComboPooledDataSource();
	static {
		try {
			comboPooledDataSourceOracle.setDriverClass("oracle.jdbc.OracleDriver"); //loads the jdbc driver
		} catch (PropertyVetoException e) {
			System.out.println("Error: unable to load Oracle jdbc driver class!");
			System.exit(1);
		}
		comboPooledDataSourceOracle.setJdbcUrl(tedConnOracle.URL);
		comboPooledDataSourceOracle.setUser(tedConnOracle.USER);
		comboPooledDataSourceOracle.setPassword(tedConnOracle.PASSWORD);

		// the settings below are optional -- c3p0 can work with defaults
		comboPooledDataSourceOracle.setMinPoolSize(5);
		comboPooledDataSourceOracle.setAcquireIncrement(5);
		comboPooledDataSourceOracle.setMaxPoolSize(50);

		try {
			comboPooledDataSourcePostgres.setDriverClass("org.postgresql.Driver"); //loads the jdbc driver
		} catch (PropertyVetoException e) {
			System.out.println("Error: unable to load Postgres jdbc driver class!");
			System.exit(1);
		}
		comboPooledDataSourcePostgres.setJdbcUrl(tedConnPostgres.URL);
		comboPooledDataSourcePostgres.setUser(tedConnPostgres.USER);
		comboPooledDataSourcePostgres.setPassword(tedConnPostgres.PASSWORD);

		// the settings below are optional -- c3p0 can work with defaults
		comboPooledDataSourcePostgres.setMinPoolSize(5);
		comboPooledDataSourcePostgres.setAcquireIncrement(5);
		comboPooledDataSourcePostgres.setMaxPoolSize(50);

		try {
			comboPooledDataSourceMysql.setDriverClass("com.mysql.cj.jdbc.Driver"); //loads the jdbc driver
		} catch (PropertyVetoException e) {
			System.out.println("Error: unable to load Postgres jdbc driver class!");
			System.exit(1);
		}
		comboPooledDataSourceMysql.setJdbcUrl(tedConnMysql.URL);
		comboPooledDataSourceMysql.setUser(tedConnMysql.USER);
		comboPooledDataSourceMysql.setPassword(tedConnMysql.PASSWORD);

		// the settings below are optional -- c3p0 can work with defaults
		comboPooledDataSourceMysql.setMinPoolSize(5);
		comboPooledDataSourceMysql.setAcquireIncrement(5);
		comboPooledDataSourceMysql.setMaxPoolSize(50);


	};

	//
	//
	//
	static DataSource dbConnectionProviderOracle() {
		try {
			Class.forName("oracle.jdbc.OracleDriver");
		} catch (ClassNotFoundException ex) {
			System.out.println("Error: unable to load Oracle jdbc driver class!");
			System.exit(1);
		}
		return comboPooledDataSourceOracle;
	}

	static DataSource dbConnectionProviderPostgres() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException ex) {
			System.out.println("Error: unable to load Postgres jdbc driver class!");
			System.exit(1);
		}
		return comboPooledDataSourcePostgres;
	}

	static DataSource dbConnectionProviderMysql() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (ClassNotFoundException ex) {
			System.out.println("Error: unable to load Postgres jdbc driver class!");
			System.exit(1);
		}
		return comboPooledDataSourceMysql;
	}

	static Properties readPropertiesFile(String propFileName) throws IOException {
		Properties properties = new Properties();
		InputStream inputStream = TestBase.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		properties.load(inputStream);
		return properties;
	}

	static void sleepMs(int ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
			throw new RuntimeException("Cannot sleep", e);
		}
	}


	public static void log(String msg){
		System.out.println(dateFormat.format(new Date()) + " " + msg);
	}

	public static void log(String pattern, Object ... args){
		String msg = MessageFormat.format(pattern, args);
		log(msg);
	}

	public static void print(String msg){
		System.out.println(msg);
	}


//	public static void printJson(Object object){
//		System.out.println(gson.toJson(object));
//	}

	public static String shortTime(Date date) {
		return dateFormat.format(date);
	}
}
