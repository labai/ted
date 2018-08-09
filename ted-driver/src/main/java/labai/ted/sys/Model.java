package labai.ted.sys;

import labai.ted.Ted.TedTask;

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
	static final String TIMEOUT_MSG = "Too long in status [work]";
	static final String BATCH_MSG = "Batch task is waiting for finish of subtasks";

	static final Set<String> nonTaskChannels = new HashSet<String>(asList(Model.CHANNEL_QUEUE, CHANNEL_PRIME));

	static class TaskRec {
		Long taskId;
		Long batchId;
		String system;
		String name;
		String status;
		String channel;
		Date nextTs;
		//String tasktp;
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

		TedTask getTedTask() {
			if (this.taskId == null)
				throw new NullPointerException("task.taskId is null");
			if (this.name == null)
				throw new NullPointerException("task.name is null");
			boolean isTimeout = msg != null && msg.startsWith(TIMEOUT_MSG);
			return new TedTask(this.taskId, this.name, this.key1, this.key2, this.data, this.retries, this.createTs, isTimeout);
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

//	enum TaskType {
//		T, 	// Task
//		S, 	// Schedule
//		B,	// Batch
//		L,  // Lock
//	}

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



		// allows letters, numbers, and ".-_"
		private static final Pattern hasInvalidCharsPattern = Pattern.compile("[^a-z0-9\\_\\-\\.]", Pattern.CASE_INSENSITIVE);
		static boolean hasInvalidChars(String str){
			Matcher m = hasInvalidCharsPattern.matcher(str);
			return m.find();
		}

		//
		// private
		//

		// allows letters and numbers
		private static final Pattern hasNonLettersPattern = Pattern.compile("[^a-z0-9]", Pattern.CASE_INSENSITIVE);
		private static boolean hasNonLetters(String str){
			Matcher m = hasNonLettersPattern.matcher(str);
			return m.find();
		}
		private static boolean hasNonAscii(String str) {
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
