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
	private final Date startTs;
	private final Long batchId;
	private final TedStatus status;
	private final boolean isNew;
	private final boolean isRetry;
	private final boolean isAfterTimeout;

	public TedTask(Long taskId, String name, String key1, String key2, String data) {
		this(taskId, name, key1, key2, data, null, 0, new Date(), new Date(), false, TedStatus.NEW);
	}

	/** for ted */
	public TedTask(Long taskId, String name, String key1, String key2, String data, Long batchId, Integer retries, Date createTs, Date startTs, boolean isAfterTimeout, TedStatus status) {
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

	public Long getTaskId() { return taskId; }
	public String getName() { return name; }
	public String getKey1() { return key1; }
	public String getKey2() { return key2; }
	public String getData() { return data; }
	public Integer getRetries() { return retries; }
	public Date getCreateTs() { return createTs; }
	/** startTs - is time, when task was taken from db, but not actually started to process it */
	public Date getStartTs() { return startTs; }
	public Long getBatchId() { return batchId; }
	public TedStatus getStatus() { return status; }

	/** is task executing first time */
	public boolean isNew() { return isNew; }
	/** is task executing not first time */
	public boolean isRetry() { return isRetry; }
	/** is task after timout (was returned from status 'WORK') */
	public boolean isAfterTimeout() { return isAfterTimeout; }


	@Override
	public String toString() {
		SimpleDateFormat iso = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
		return "TedTask{" + name + " " + taskId + " " + (isRetry?"R"+retries:"") + (isAfterTimeout ?"T":"") + (isNew?"N":"") + (key1==null?"":" key1='"+key1+'\'') + (key2==null?"":" key2='"+key2+'\'') + " createTs=" + (createTs==null?"null":iso.format(createTs)) + (batchId == null?"":" batchId="+batchId) + '}';
	}
}
