package ted.driver.sys;

import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author Augustus
 *         created on 2016.10.19
 */
class MiscUtils {

	static String toTimeString(Date date){
		if (date == null) return "";
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
	}

	static String toDateString(Date date){
		if (date == null) return "";
		return new SimpleDateFormat("yyyy-MM-dd").format(date);
	}

	// for logging
	static String dateToStrTs(long dateMs) {
		SimpleDateFormat df = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSS");
		return df.format(new Date(dateMs));
	}

	static String generateInstanceId() {
		// hostname with pid
		String id = ManagementFactory.getRuntimeMXBean().getName();
		if (id == null)
			id = "x";
		Random random = new Random();
		id += "#" + Integer.toString(Math.abs(random.nextInt()), 36);
		return id;
	}

	static <T> T nvl (T obj, T altObj) {
		return obj == null ? altObj : obj;
	}

	static String nvle (String str) {
		return str == null ? "" : str;
	}

	public static <T> List<T> asList(T ... a) {
		return Arrays.asList(a);
	}
	public static <T> List<T> asList(T a) {
		return Collections.singletonList(a);
	}
	public static <T> List<T> asList() {
		return Collections.emptyList();
	}

}
