package ted.driver.sys;

import ted.driver.Ted.TedStatus;
import ted.driver.TedTask;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 */
class Model {
	static final String CHANNEL_MAIN = "MAIN";
	static final String CHANNEL_QUEUE = "TedEQ"; // Event Queue
	static final String CHANNEL_PRIME = "TedNO"; // Non Operating - just simple rec, e.g. for prime instance
	static final String CHANNEL_SYSTEM = "TedSS"; // System Services - for internal tasks, just ThreadPoolExecutor
	static final String CHANNEL_BATCH = "TedBW"; // Batch Wait
	static final String CHANNEL_NOTIFY = "TedIN"; // Instance Notification
	static final String TIMEOUT_MSG = "Too long in status [work]";
	static final String BATCH_MSG = "Batch task is waiting for finish of subtasks";
	static final String REJECTED_MSG = "Rejected by taskExecutor";

	static final Set<String> nonTaskChannels = new HashSet<String>(asList(Model.CHANNEL_QUEUE, CHANNEL_PRIME, CHANNEL_BATCH, CHANNEL_NOTIFY, CHANNEL_SYSTEM));

	static class TaskRec {
		Long taskId;
		Long batchId;
		String system;
		String name;
		String status;
		String channel;
		Date nextTs;
		String msg;
		Integer retries;
		String key1;
		String key2;
		String data;
		Date createTs;
		Date startTs;
		Date finishTs;

		@Override
		public String toString() {
			return "TaskRec{taskId=" + taskId + " system=" + system + " name=" + name + " status=" + status + '}';
		}

		// create hard copy
		TedTask getTedTask() {
			if (this.taskId == null)
				throw new NullPointerException("task.taskId is null");
			if (this.name == null)
				throw new NullPointerException("task.name is null");
			boolean isTimeout = msg != null && msg.startsWith(TIMEOUT_MSG);
			TedStatus status = null;
			try { status = TedStatus.valueOf(this.status); } catch (IllegalArgumentException e) { }
			return new TedTaskImpl(this.taskId, this.name, this.key1, this.key2, this.data, this.batchId, this.retries, this.createTs, this.startTs, isTimeout, status);
		}

	}

	static class TedTaskImpl implements TedTask {
		private final Long taskId;
		private final String name;
		private final String key1;
		private final String key2;
		private final String data;
		private final Integer retries;
		private final Date createTs;
		private final Date startTs;
		private final Long batchId;
		private final TedStatus status;
		private final boolean isNew;
		private final boolean isRetry;
		private final boolean isAfterTimeout;

		public TedTaskImpl(Long taskId, String name, String key1, String key2, String data) {
			this(taskId, name, key1, key2, data, null, 0, new Date(), new Date(), false, TedStatus.NEW);
		}

		/** for ted */
		public TedTaskImpl(Long taskId, String name, String key1, String key2, String data, Long batchId, Integer retries, Date createTs, Date startTs, boolean isAfterTimeout, TedStatus status) {
			if (status == null)
				status = TedStatus.NEW;
			boolean work = status == TedStatus.WORK;
			this.taskId = taskId;
			this.name = name;
			this.key1 = key1;
			this.key2 = key2;
			this.data = data;
			this.retries = retries;
			this.createTs = createTs == null ? null : new Date(createTs.getTime());
			this.startTs = startTs == null ? null : new Date(startTs.getTime());
			this.batchId = batchId;
			this.status = status;
			this.isRetry = status == TedStatus.RETRY || (work && retries != null && retries > 0);
			this.isAfterTimeout = (status == TedStatus.RETRY || work) && isAfterTimeout;
			this.isNew = status == TedStatus.NEW || (work && !(this.isRetry || this.isAfterTimeout));
		}

		@Override public Long getTaskId() { return taskId; }
		@Override public String getName() { return name; }
		@Override public String getKey1() { return key1; }
		@Override public String getKey2() { return key2; }
		@Override public String getData() { return data; }
		@Override public Integer getRetries() { return retries; }
		@Override public Date getCreateTs() { return createTs; } /** startTs - is time, when task was taken from db, but not actually started to process it */
		@Override public Date getStartTs() { return startTs; }
		@Override public Long getBatchId() { return batchId; }
		@Override public TedStatus getStatus() { return status; }

		/** is task executing first time */
		@Override public boolean isNew() { return isNew; }
		/** is task executing not first time */
		@Override public boolean isRetry() { return isRetry; }
		/** is task after timout (was returned from status 'WORK') */
		@Override public boolean isAfterTimeout() { return isAfterTimeout; }


		@Override
		public String toString() {
			SimpleDateFormat iso = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
			return "TedTask{" + name + " " + taskId + " " + (isRetry?"R"+retries:"") + (isAfterTimeout ?"T":"") + (isNew?"N":"") + (key1==null?"":" key1='"+key1+'\'') + (key2==null?"":" key2='"+key2+'\'') + " createTs=" + (createTs==null?"null":iso.format(createTs)) + (batchId == null?"":" batchId="+batchId) + '}';
		}
	}



	static class TaskParam {
		Long taskId;
		Long batchId;
		String name;
		String channel;
		String key1;
		String key2;
		String data;
	}

	private static class Lengths {
		static final int len_system = 8;
		static final int len_name = 15;
		static final int len_status = 5;
		static final int len_channel = 5;
		static final int len_msg = 300;
		static final int len_key1 = 30;
		static final int len_key2 = 30;
		static final int len_data = 500; // not used
	}

	static class FieldValidateException extends IllegalArgumentException {
		public FieldValidateException(String s) {
			super(s);
		}
	}

	static class FieldValidator {
		public static boolean isEmpty(String str) { return str == null || str.isEmpty(); }
		// allows letters, numbers, and ".-_"
		private static final Pattern hasInvalidCharsPattern = Pattern.compile("[^a-z0-9\\_\\-\\.]", Pattern.CASE_INSENSITIVE);

		public static void validateTaskKey1(String key1) {
			checkMaxLengthAscii("key1", key1, Lengths.len_key1);
		}
		public static void validateTaskKey2(String key2) {
			checkMaxLengthAscii("key2", key2, Lengths.len_key2);
		}
		public static void validateTaskData(String data) {
			/* checkMaxLengthAscii("data", data, Lengths.len_data); */
		}
		public static void validateTaskSystem(String system) {
			if (system == null || system.isEmpty())
				throw new FieldValidateException("System id is required");
			if (system.length() > Lengths.len_system)
				throw new FieldValidateException("System id length must be <= " + Lengths.len_system + ", systemId=" + system);
			if (hasInvalidChars(system))
				throw new FieldValidateException("System id has invalid character, allowed letters, numbers, and \".-_\", systemId=" + system);
			if (beginsWithIgnoreCase(system, "TED"))
				throw new IllegalArgumentException("System id is reserved (TED*), system=" + system);
		}
		public static void validateTaskName(String taskName) {
			if (taskName == null || taskName.isEmpty())
				throw new FieldValidateException("Task name is required");
			if (taskName.length() > Lengths.len_name)
				throw new FieldValidateException("Task name length must be <= " + Lengths.len_name + ", task=" + taskName);
			if (hasInvalidChars(taskName))
				throw new FieldValidateException("Task name has invalid character, allowed letters, numbers, and \".-_\", task=" + taskName);
			if (beginsWithIgnoreCase(taskName, "TED"))
				throw new IllegalArgumentException("Task name is reserved (TED*), task=" + taskName);
		}
		public static void validateTaskChannel(String channel) {
			if (channel == null || channel.isEmpty())
				throw new FieldValidateException("Channel name is required");
			if (channel.length() > Lengths.len_channel)
				throw new FieldValidateException("Channel name length must be maximum " + Lengths.len_channel + " symbols length, channel=" + channel);
			if (hasNonLetters(channel))
				throw new IllegalArgumentException("Channel name has invalid character, allowed letters and numbers, channel=" + channel);
			if (beginsWithIgnoreCase(channel, "TED"))
				throw new IllegalArgumentException("Channel name is reserved (TED*), channel=" + channel);
		}

		static boolean hasInvalidChars(String str){
			Matcher m = hasInvalidCharsPattern.matcher(str);
			return m.find();
		}

		//
		// private
		//

		// allows letters and numbers
		private static final Pattern hasNonLettersPattern = Pattern.compile("[^a-z0-9]", Pattern.CASE_INSENSITIVE);
		static boolean hasNonLetters(String str){
			Matcher m = hasNonLettersPattern.matcher(str);
			return m.find();
		}
		static boolean hasNonAscii(String str) {
			if (str == null)
				return false;
			return !str.matches("\\A\\p{ASCII}*\\z");
		}

		private static boolean beginsWithIgnoreCase(String str, String begins) {
			if (str == null || begins == null)
				return false;
			if (str.length() < begins.length())
				return false;
			return str.substring(0, begins.length() - 1).equalsIgnoreCase(begins);
		}

		private static void checkMaxLengthAscii(String paramName, String str, int maxLen) {
			if (str == null)
				return;
			if (str.length() > maxLen)
				throw new FieldValidateException("Parameter's '" + paramName + "' length must be less or equal to " + maxLen + ", but got " + str.length() + ", value='" + str + "'");
			if (hasNonAscii(str))
				throw new FieldValidateException("Parameter's '" + paramName + "' has non ASCII letters, value='" + str + "'");
		}

	}

}
