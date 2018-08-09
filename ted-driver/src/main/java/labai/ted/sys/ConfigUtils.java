package labai.ted.sys;

import labai.ted.Ted.TedRetryScheduler;
import labai.ted.sys.RetryConfig.PeriodPatternRetryScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2016.10.17
 *
 * for TED internal usage only!!!
 *
 * config precedence:
 * 		- from <ted>.properties (highest)
 * 		- from method params (from app java code)
 *		- for task - from ted.properties default settings (when such exists such, like default retryPattern)
 *		- ted defaults (lowest)
 *
 */
class ConfigUtils {
	private final static Logger logger = LoggerFactory.getLogger(ConfigUtils.class);
	private final static Logger loggerConfig = LoggerFactory.getLogger("ted-config");

	static final TedRetryScheduler BATCH_RETRY_SCHEDULER = new PeriodPatternRetryScheduler("1s*10,2s*5,3s*4,4s*2,5s*4,6s*10,10s*48,20s*30,30s*100,60s*1440");

	static final String PROPERTY_PREFIX_CHANNEL = "ted.channel.";
	static final String PROPERTY_PREFIX_TASK = "ted.task.";

	static class TedProperty {
		public static final String SYSTEM_ID 						= "ted.systemId";

		// driver internals
		public static final String DRIVER_INIT_DELAY_MS 			= "ted.driver.initDelayMs";
		public static final String DRIVER_INTERVAL_DRIVER_MS 		= "ted.driver.intervalDriverMs";
		public static final String DRIVER_INTERVAL_MAINTENANCE_MS 	= "ted.driver.intervalMaintenanceMs";
		// maintenance
		public static final String DRIVER_OLD_TASK_ARCHIVE_DAYS		= "ted.maintenance.oldTaskArchiveDays";

		// task defaults
		public static final String TASKDEF_RETRY_PAUSES 			= "ted.taskDefault.retryPauses";
		public static final String TASKDEF_TIMEOUT_MINUTES 			= "ted.taskDefault.timeoutMinutes";
		public static final String TASKDEF_BATCH_TIMEOUT_MINUTES	= "ted.taskDefault.batchTimeoutMinutes";

		// short channel properties (w/o prefix "ted.channel.<CHANNEL>.")
		public static final String CHANNEL_WORKERS_COUNT = "workerCount";
		//public static final String CHANNEL_WORKERS_MAX = "workersMax";
		public static final String CHANNEL_TASK_BUFFER	 = "taskBuffer";
		public static final String CHANNEL_PRIME_ONLY	 = "primeOnly";

		// short task properties (w/o prefix "ted.task.<TASK>.")
		public static final String TASK_TIMEOUT_MINUTES			= "timeoutMinutes";
		public static final String TASK_RETRY_PAUSES			= "retryPauses";
		public static final String TASK_CHANNEL					= "channel";
		public static final String TASK_TYPE					= "taskType";
		public static final String TASK_BATCH_TASK				= "batchTask";
		public static final String TASK_BATCH_TIMEOUT_MINUTES	= "batchTimeoutMinutes";

	}
	static class TedConfig {
		private String defaultRetryPauses = "12s,36s,90s,300s,16m,50m,2h,5h,7h*5;dispersion=10";
		private int defaultTaskTimeoutMn = 30; // 30min
		private int defaultBatchTaskTimeoutMn = 180; // 3h
		private int initDelayMs = 5000;
		private int intervalDriverMs = 700;
		private int intervalMaintenanceMs = 10000;
		private int oldTaskArchiveDays = 35;
		private Map<String, Properties> channelMap = new HashMap<String, Properties>();
		private Map<String, Properties> taskMap = new HashMap<String, Properties>();
		private final String systemId;
		private final String instanceId;

		public TedConfig(String systemId) {
			this.systemId = systemId;
			this.instanceId = MiscUtils.generateInstanceId();
		}

		// getters
		public String defaultRetryPauses() { return defaultRetryPauses; }
		public int defaultTaskTimeoutMn() { return defaultTaskTimeoutMn; }
		public int defaultBatchTaskTimeoutMn() { return defaultBatchTaskTimeoutMn; }
		public int initDelayMs() { return initDelayMs; }
		public int intervalDriverMs() { return intervalDriverMs; }
		public int intervalMaintenanceMs() { return intervalMaintenanceMs; }
		public int oldTaskArchiveDays() { return oldTaskArchiveDays; }
		public Map<String, Properties> channelMap() { return Collections.unmodifiableMap(channelMap); }
		public Map<String, Properties> taskMap() { return Collections.unmodifiableMap(taskMap); }
		public String systemId() { return systemId; }
		public String instanceId() { return instanceId; }
	}


	public static void readTedProperties(TedConfig config, Properties properties) {
		if (properties == null || properties.isEmpty()) {
			logger.info("Not ted properties was provided, using default configuration");
			return;
		}
		String sv;
		Integer iv;

		iv = getInteger(properties, TedProperty.DRIVER_INIT_DELAY_MS, null);
		if (iv != null && iv >= 0)
			config.initDelayMs = iv;
		iv = getInteger(properties, TedProperty.DRIVER_INTERVAL_DRIVER_MS, null);
		if (iv != null)
			config.intervalDriverMs = Math.max(iv, 100);
		iv = getInteger(properties, TedProperty.DRIVER_INTERVAL_MAINTENANCE_MS, null);
		if (iv != null)
			config.intervalMaintenanceMs = Math.max(iv, 100);

		iv = getInteger(properties, TedProperty.DRIVER_OLD_TASK_ARCHIVE_DAYS, null);
		if (iv != null)
			config.oldTaskArchiveDays = iv;

		sv = getString (properties, TedProperty.TASKDEF_RETRY_PAUSES, null);
		if (sv != null)
			config.defaultRetryPauses = sv;
		iv = getInteger(properties, TedProperty.TASKDEF_TIMEOUT_MINUTES, null);
		if (iv != null && iv > 0)
			config.defaultTaskTimeoutMn = Math.min(iv, 24 * 3600);
		iv = getInteger(properties, TedProperty.TASKDEF_BATCH_TIMEOUT_MINUTES, null);
		if (iv != null && iv > 0)
			config.defaultBatchTaskTimeoutMn = Math.min(iv, 24 * 3600);


		// channels and tasks config
		config.channelMap.putAll(getShortPropertiesByPrefix(properties, PROPERTY_PREFIX_CHANNEL));
		config.taskMap.putAll(getShortPropertiesByPrefix(properties, PROPERTY_PREFIX_TASK));
	}

	static void printConfigToLog(TedConfig config) {
		loggerConfig.info("driver:"
				+ " systemId=" + config.systemId
				+ " initDelayMs=" + config.initDelayMs
				+ " intervalDriverMs=" + config.intervalDriverMs
				+ " intervalMaintenanceMs=" + config.intervalMaintenanceMs
				);
		loggerConfig.info("maintenance:"
				+ " oldTaskArchiveDays=" + config.oldTaskArchiveDays
				);
		loggerConfig.info("taskDefault:"
				+ " timeoutMinutes=" + config.defaultTaskTimeoutMn
				+ " retryPattern=" + config.defaultRetryPauses
				);
		loggerConfig.info("channels: " + config.channelMap.keySet().toString());
	}

	static Map<String, Properties> getShortPropertiesByPrefix(Properties properties, String prefix) {
		Map<String, Properties> shortPropMap = new HashMap<String, Properties>();
		if (properties == null) {
			return shortPropMap;
		}
		for (Object okey : properties.keySet()) {
			String key = okey.toString();
			if (!key.startsWith(prefix))
				continue;
			String tail = key.substring(prefix.length());
			int nextDot = tail.indexOf(".");
			String group, shortKey;
			if (nextDot < 0) {
				group = tail;
				shortKey = null;
			} else {
				group = tail.substring(0, nextDot);
				shortKey = tail.substring(nextDot + 1);
			}
			Properties shortProp = shortPropMap.get(group);
			if (shortProp == null) {
				shortProp = new Properties();
				shortPropMap.put(group, shortProp);
			}
			if (shortKey != null) {
				shortProp.put(shortKey, properties.get(okey));
			}
		}
		return shortPropMap;
	}

	// returns int value of property or defaultValue if not found or invalid
	static Integer getInteger(Properties properties, String key, Integer defaultValue) {
		if (properties == null)
			return defaultValue;
		String value = properties.getProperty(key);
		if (value == null || value.isEmpty())
			return defaultValue;
		int intVal;
		try {
			intVal = Integer.parseInt(value);
		} catch (NumberFormatException e) {
			logger.warn("Cannot read property '" + key + "'. Expected integer, but got '" + value + "'. Setting default value = '" + defaultValue + "'", e.getMessage());
			return defaultValue;
		}
		logger.trace("Read property '" + key + "' value '" + intVal + "'");
		return intVal;
	}

	static String getString(Properties properties, String key, String defaultValue) {
		if (properties == null)
			return defaultValue;
		String value = properties.getProperty(key, defaultValue);
		logger.trace("Read property '" + key + "' value '" + value + "'");
		return value;
	}



}
