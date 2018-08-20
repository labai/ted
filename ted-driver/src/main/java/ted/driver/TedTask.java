package ted.driver;

import ted.driver.Ted.TedStatus;

import java.text.SimpleDateFormat;
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
	private final Long batchId;
	private final TedStatus status;
	private final boolean isNew;
	private final boolean isRetry;
	private final boolean isAfterTimeout;

	public TedTask(Long taskId, String name, String key1, String key2, String data) {
		this(taskId, name, key1, key2, data, null, 0, new Date(), false, TedStatus.NEW);
	}

	public TedTask(Long taskId, String name, String key1, String key2, String data, Long batchId, Integer retries, Date createTs, boolean isAfterTimeout, TedStatus status) {
		if (status == null)
			status = TedStatus.NEW;
		boolean work = status == TedStatus.WORK;
		this.taskId = taskId;
		this.name = name;
		this.key1 = key1;
		this.key2 = key2;
		this.data = data;
		this.retries = retries;
		this.createTs = createTs;
		this.batchId = batchId;
		this.status = status;
		this.isRetry = status == TedStatus.RETRY || (work && retries != null && retries > 0);
		this.isAfterTimeout = (status == TedStatus.RETRY || work) && isAfterTimeout;
		this.isNew = status == TedStatus.NEW || (work && !(this.isRetry || this.isAfterTimeout));
	}

	public Long getTaskId() { return taskId; }
	public String getName() { return name; }
	public String getKey1() { return key1; }
	public String getKey2() { return key2; }
	public String getData() { return data; }
	public Integer getRetries() { return retries; }
	public Date getCreateTs() { return createTs; }
	public Long getBatchId() { return batchId; }
	public TedStatus getStatus() { return status; }
	public boolean isRetry() { return isRetry; }
	public boolean isAfterTimeout() { return isAfterTimeout; }
	public boolean isNew() { return isNew; }

	@Override
	public String toString() {
		SimpleDateFormat iso = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		return "TedTask{" + name + " " + taskId + " " + (isRetry?"R"+retries:"") + (isAfterTimeout ?"T":"") + (isNew?"N":"") + (key1==null?"":" key1='"+key1+'\'') + (key2==null?"":" key2='"+key2+'\'') + " createTs=" + (createTs==null?"null":iso.format(createTs)) + (batchId == null?"":" batchId="+batchId) + '}';
	}
}
