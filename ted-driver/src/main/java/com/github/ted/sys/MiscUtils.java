package com.github.ted.sys;

import java.text.SimpleDateFormat;
import java.util.Date;

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

}
