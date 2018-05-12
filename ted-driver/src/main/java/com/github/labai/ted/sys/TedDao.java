package com.github.labai.ted.sys;

import com.github.labai.ted.Ted.TedStatus;
import com.github.labai.ted.sys.Model.TaskParam;
import com.github.labai.ted.sys.Model.TaskRec;
import com.github.labai.ted.sys.TedDaoAbstract.DbType;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 */
interface TedDao {
	DbType getDbType();

	Long createTask(String name, String channel, String data, String key1, String key2, Long batchId);

	Long createTaskPostponed(String name, String channel, String data, String key1, String key2, int postponeSec);

	Long createTaskWithWorkStatus(String name, String channel, String data, String key1, String key2);

	List<Long> createTasksBulk(List<TaskParam> taskParams);

	void processMaintenanceFrequent();

	void processMaintenanceRare(int deleteAfterDays);

	List<TaskRec> getWorkingTooLong();

	void setTaskPlannedWorkTimeout(long taskId, Date timeoutTime);

	// quick check, is there any task
	List<String> getWaitChannels();

	List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes);

	void setStatus(long taskId, TedStatus status, String msg);

	void setStatusPostponed(long taskId, TedStatus status, String msg, Date nextRetryTs);

	TaskRec getTask(long taskId);

	boolean checkIsBatchFinished(long batchId);

	void cleanupRetries(Long taskId, String msg);

	Map<TedStatus, Integer> getBatchStatusStats(long batchId);

}
