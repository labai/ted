package sample3.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ted.driver.Ted.TedDbType;
import ted.driver.TedDriver;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2018.08.25
 */
@Configuration
public class TedConfig {
	public static final String SAMPLE_TASK_NAME = "PROCESS_LINE";

	@Autowired
	private DataSource dataSource;

	@Bean(destroyMethod="shutdown")
	public TedDriver tedDriver() {
		Properties properties = new Properties();
		String propFileName = "ted.properties";
		InputStream inputStream = TedConfig.class.getClassLoader().getResourceAsStream(propFileName);
		if (inputStream == null)
			throw new RuntimeException("Property file '" + propFileName + "' not found in the classpath");
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			throw new RuntimeException("Cannot read property file '" + propFileName + "'", e);
		}
		if (dataSource == null)
			throw new IllegalStateException("dataSource is null");
		TedDriver tedDriver = new TedDriver(TedDbType.POSTGRES, dataSource, properties);
		tedDriver.start();
		return tedDriver;
	}
}
