package ted.driver.sys;

import com.zaxxer.hikari.HikariDataSource;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import ted.driver.Ted.TedDbType;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2016.09.22
 */
class TestConfig {
    static final boolean INT_TESTS_ENABLED = true;
    static final String SYSTEM_ID = "ted.test";
    static final TedDbType testDbType = TedDbType.HSQLDB; // which one we are testing

    private static final DataSource dataSource = initDataSource(testDbType);

    static DataSource getDataSource() {
        return dataSource;
    }

    private static DataSource initDataSource(TedDbType dbType) {
        HikariDataSource dataSource = null;
        try {
            Properties properties = TestUtils.readPropertiesFile("application-test.properties");
            String prefix = "db." + dbType.toString().toLowerCase();
            String driver = properties.getProperty(prefix + ".driver");
            String url = properties.getProperty(prefix + ".url");
            String user = properties.getProperty(prefix + ".user");
            String password = properties.getProperty(prefix + ".password");
            String initScript = properties.getProperty(prefix + ".initScript");
            Class.forName(driver);
            dataSource = new HikariDataSource();
            dataSource.setJdbcUrl(url);
            dataSource.setUsername(user);
            dataSource.setPassword(password);
            if (initScript != null && ! initScript.isEmpty()) {
                executeInitScript(initScript, dataSource);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (ClassNotFoundException ex) {
            System.out.println("Error: unable to load " + dbType + " jdbc driver class!");
            System.exit(1);
        }
        return dataSource;
    }

    private static void executeInitScript(String scriptFile, DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()){
            InputStream inputStream = TestConfig.class.getClassLoader().getResourceAsStream(scriptFile);
            SqlFile sqlFile = new SqlFile(new InputStreamReader(inputStream), "init", System.out, "UTF-8", false, new File("."));
            sqlFile.setConnection(connection);
            sqlFile.execute();
        } catch (SQLException | SqlToolError | IOException e) {
            e.printStackTrace();
        }
    }

}
