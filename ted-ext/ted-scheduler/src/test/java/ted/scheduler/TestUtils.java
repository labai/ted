package ted.scheduler;

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
		System.out.println(new SimpleDateFormat("mm:ss.SSS").format(new Date()) + " " + msg);
	}

	public static void log(String pattern, Object ... args){
		String msg = MessageFormat.format(pattern, args);
		log(msg);
	}

	public static void print(String msg){
		System.out.println(msg);
	}

}
