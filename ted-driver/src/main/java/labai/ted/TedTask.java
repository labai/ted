package labai.ted;

import java.util.Date;

/**
 * @author Augustus
 *         created on 2016.09.12
 *
 * TedTask object will be passed to processor.
 */
public class TedTask {
	private final Long taskId;
	private final String name;
	private final String key1;
	private final String key2;
	private final String data;
	private final Integer retries;
	private final Date createTs;
	private final boolean isNew;
	private final boolean isRetry;
	private final boolean isAfterTimeout;

	public TedTask(Long taskId, String name, String key1, String key2, String data) {
		this(taskId, name, key1, key2, data, 0, new Date(), false);
	}

	public TedTask(Long taskId, String name, String key1, String key2, String data, Integer retries, Date createTs, boolean isAfterTimeout) {
		this.taskId = taskId;
		this.name = name;
		this.key1 = key1;
		this.key2 = key2;
		this.data = data;
		this.retries = retries;
		this.createTs = createTs;
		this.isRetry = retries != null && retries > 0;
		this.isAfterTimeout = isAfterTimeout;
		this.isNew = ! (this.isRetry || this.isAfterTimeout);
	}

	public Long getTaskId() { return taskId; }
	public String getName() { return name; }
	public String getKey1() { return key1; }
	public String getKey2() { return key2; }
	public String getData() { return data; }
	public Integer getRetries() { return retries; }
	public Date getCreateTs() { return createTs; }
	public boolean isRetry() { return isRetry; }
	public boolean isAfterTimeout() { return isAfterTimeout; }
	public boolean isNew() { return isNew; }

	@Override
	public String toString() {
		return "TedTask{" + name + " " + taskId + " key1='" + key1 + '\'' + " key2='" + key2 + '\'' + " retries=" + retries + " createTs=" + createTs + " is=" + (isRetry?"R":"") + (isAfterTimeout ?"T":"") + (isNew?"N":"") + '}';
	}
}
