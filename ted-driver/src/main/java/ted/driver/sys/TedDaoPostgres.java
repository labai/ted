package ted.driver.sys;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.JdbcSelectTed.SqlParam;
import ted.driver.sys.JdbcSelectTed.TedSqlDuplicateException;
import ted.driver.sys.JdbcSelectTed.TedSqlException;
import ted.driver.sys.Model.TaskParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.PrimeInstance.CheckPrimeParams;
import ted.driver.sys.QuickCheck.CheckResult;
import ted.driver.sys.SqlUtils.DbType;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static ted.driver.sys.JdbcSelectTed.sqlParam;
import static ted.driver.sys.MiscUtils.asList;
import static ted.driver.sys.MiscUtils.nvle;

/**
 * @author Augustus
 * created on 2017.04.14
 * <p>
 * for TED internal usage only!!!
 */
class TedDaoPostgres extends TedDaoAbstract implements TedDaoExt {
    private static final Logger logger = LoggerFactory.getLogger(TedDaoPostgres.class);

    private static final int CHANNEL_SUBSELECT_CHUNK = 10;
    private static final String INDEX_QUICKCHK = "ix_tedtask_quickchk";

    public TedDaoPostgres(String system, DataSource dataSource, Stats stats, String schema, String tableName) {
        super(system, dataSource, DbType.POSTGRES, stats, schema, tableName);
    }

    private static class TaskIdRes {
        Long taskid;
    }

    @Override
    public List<CheckResult> quickCheck(CheckPrimeParams checkPrimeParams, boolean skipChannelCheck) {
        String sql = "";
        String logId = "";

        if (checkPrimeParams != null) {
            String sqlPrime;
            if (checkPrimeParams.isPrime()) {
                sqlPrime = ""
                    + "with updatedPrimeTask as ("
                    + "  update $tedTask set finishts = $now + $intervalSec"
                    + "  where taskid = $primeTaskId"
                    + "  and system = '$sys'"
                    + "  and data = '$instanceId'"
                    + "  returning 'PRIME'::text as result"
                    + ")"
                    + " select case when exists(select * from updatedPrimeTask) then 'PRIME' else 'LOST_PRIME' end as name, 'PRIM' as type, null::timestamp as tillts, 0 as taskCnt";
                logId = "b";
            } else {
                sqlPrime = "select case when finishts < $now then 'CAN_PRIME' else 'NEXT_CHECK' end as name, 'PRIM' as type, finishts as tillts, 0 as taskCnt"
                    + " from $tedTask"
                    + " where taskid = $primeTaskId and system = '$sys'";
                logId = "c";
            }
            sqlPrime = sqlPrime.replace("$tedTask", fullTableName);
            sqlPrime = sqlPrime.replace("$intervalSec", dbType.sql().intervalSeconds(checkPrimeParams.postponeSec()));
            sqlPrime = sqlPrime.replace("$instanceId", checkPrimeParams.instanceId());
            sqlPrime = sqlPrime.replace("$primeTaskId", Long.toString(checkPrimeParams.primeTaskId()));
            sql += sqlPrime;
        }

        // check for new tasks
        if (! skipChannelCheck) {
            if (! sql.isEmpty())
                sql += " union all ";
            sql += "select channel as name, 'CHAN' as type, null::timestamp as tillts, count(*) as taskCnt "
                + " from $tedTask"
                + " where system = '$sys' and nextTs <= $now"
                + " group by channel";
            logId = "a" + logId;
        }

        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$now", dbType.sql().now());

        if (sql.isEmpty())
            return Collections.emptyList();

        return selectData("qckchk_" + logId, sql, CheckResult.class, Collections.emptyList());
    }

    @Override
    public List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes) {
        // limit channel subselect count in one select
        Collection<List<Entry<String, Integer>>> chunked = splitList(channelSizes.entrySet(), CHANNEL_SUBSELECT_CHUNK);
        List<TaskRec> reservedTasks = new ArrayList<>();
        for (List<Entry<String, Integer>> chunk : chunked) {
            List<TaskRec> res = reserveTaskPortionChunk(chunk);
            reservedTasks.addAll(res);
        }
        return reservedTasks;
    }

    static <T> Collection<List<T>> splitList(Collection<T> list, int chunkSize) {
        if (chunkSize <= 0)
            throw new IllegalArgumentException("Chunk size must be > 0");
        final AtomicInteger counter = new AtomicInteger();
        return list.stream()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
            .values();
    }

    private List<TaskRec> reserveTaskPortionChunk(List<Entry<String, Integer>> channelSizes) {
        // create as many subselects as there are channels
        String ptr = " select taskid from ("
            + " select taskid from $tedTask "
            + " where status in ('NEW','RETRY') and $systemCheck and channel = ?"
            + " and nextTs < $now"
            + " limit ?"
            + " $FOR_UPDATE_SKIP_LOCKED"
            + ") ";
        ptr = ptr.replace("$tedTask", fullTableName);
        ptr = ptr.replace("$now", dbType.sql().now());
        ptr = ptr.replace("$systemCheck", systemCheck);
        ptr = ptr.replace("$FOR_UPDATE_SKIP_LOCKED", dbType.sql().forUpdateSkipLocked());

        int i = 0;
        StringBuilder sb = new StringBuilder();
        List<SqlParam> params = new ArrayList<>();
        for (Entry<String, Integer> entry : channelSizes) {
            i++;
            if (i > 1)
                sb.append(" union all ");
            sb.append(ptr).append(" ss").append(i);
            params.add(sqlParam(entry.getKey(), JetJdbcParamType.STRING)); // key=channel
            params.add(sqlParam(entry.getValue(), JetJdbcParamType.INTEGER)); //value=count
        }

        String sqlLogId = "reserve_chan_pg(" + channelSizes.size() + ")";
        String sql = "update $tedTask set status = 'WORK', startTs = $now, nextTs = null"
            + " where status in ('NEW','RETRY') and $systemCheck"
            + " and taskid in ("
            + " $taskIdSubSelects"
            + " )"
            + " returning *";

        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$now", dbType.sql().now());
        sql = sql.replace("$systemCheck", systemCheck);
        sql = sql.replace("$taskIdSubSelects", sb.toString());

        return selectData(sqlLogId, sql, TaskRec.class, params);
    }


    @Override
    public List<Long> createTasksBulk(List<TaskParam> taskParams) {
        ArrayList<Long> taskIds = getSequencePortion("SEQ_TEDTASK_ID", taskParams.size());
        int iNum = 0;

        for (TaskParam param : taskParams) {
            param.taskId = taskIds.get(iNum++);
        }

        try (Connection connection = dataSource.getConnection()) {
            executePgCopy(connection, taskParams);
        } catch (SQLException e) {
            throw new TedSqlException("can't execute pgCopy", e);
        }

        return taskIds;
    }

    //
    // ext
    //

    // reserve concrete task. if can't - return null, else taskRec
    // used in eventQueue
    public TaskRec eventQueueReserveTask(long taskId) {
        String sqlLogId = "reserve_task";
        String sql = "update $tedTask set status = 'WORK', startTs = $now, nextTs = null"
            + " where status in ('NEW','RETRY') and system = '$sys'"
            + " and taskid in ("
            + " select taskid from $tedTask "
            + " where status in ('NEW','RETRY') and system = '$sys'"
            + " and taskid = ?"
            + " for update skip locked"
            + ") returning $tedTask.*"
            ;
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$now", dbType.sql().now());
        sql = sql.replace("$sys", thisSystem);
        List<TaskRec> tasks = selectData(sqlLogId, sql, TaskRec.class, asList(
            sqlParam(taskId, JetJdbcParamType.LONG)
        ));
        return tasks.isEmpty() ? null : tasks.get(0);
    }


    //
    // prime
    //

    @Override
    public Long findPrimeTaskId() {
        String sql;
        sql = "select taskid from $tedTask where system = '$sys' and name = 'TED_PRIME' limit 2";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        List<TaskIdRes> ls2 = selectData("find_primetask", sql, TaskIdRes.class, Collections.emptyList());
        if (ls2.size() == 1) {
            logger.debug("found primeTaskId={}", ls2.get(0).taskid);
            return ls2.get(0).taskid;
        }
        if (ls2.size() > 1)
            throw new IllegalStateException("found few primeTaskId tasks for system=" + thisSystem + " (name='TED_PRIME'). Ther should be only 1. Please delete them and restart again");

        // not exists yet - try to create new. alternatively it is possible create manually by deployment script.
        // this part will be executed only once per live of system..

        // get next taskid, prefer some from lower numbers (11..99)
        String sqlNextId = "coalesce("
            + "  nullif(coalesce((select max(taskid) from $tedTask where taskid between 10 and 99), 10) + 1, 100),"
            + "  $sequenceTedTask"
            + "  )";
        sql = "insert into $tedTask(taskid, system, name, status, channel, startts, nextts, msg)"
            + " values (" + sqlNextId + ", '$sys', 'TED_PRIME', 'SLEEP', '$channel', $now, null, 'This is internal TED pseudo-task for prime check')";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$now", dbType.sql().now());
        sql = sql.replace("$channel", Model.CHANNEL_PRIME);
        sql = sql.replace("$sequenceTedTask", dbType.sql().sequenceSql(schemaPrefix() + "SEQ_TEDTASK_ID"));

        execute("insert_prime", sql, Collections.emptyList());

        // check again, to be sure
        sql = "select taskid from $tedTask where system = '$sys' and name = 'TED_PRIME'";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        Long taskId = selectSingleLong("find_primetask(2)", sql);
        if (taskId == null)
            throw new IllegalStateException("Something went wrong, please try to create manually 'TED_PRIME' task for system '" + thisSystem + "'");

        return taskId;
    }

    @Override
    public boolean becomePrime(Long primeTaskId, String instanceId) {
        String sql = "update $tedTask set data = '$instanceId', finishts = now() + interval '5 seconds'"
            + " where taskid = (select taskid from $tedTask where system = '$sys' "
            + "  and (finishts < now() or finishts is null) "
            + "  and taskid = $primeTaskId for update skip locked)"
            + " returning taskid";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$primeTaskId", primeTaskId.toString());
        sql = sql.replace("$instanceId", instanceId);

        List<TaskIdRes> res = selectData("take_prime", sql, TaskIdRes.class, Collections.emptyList());
        return res.size() == 1;
    }

    //
    // queue events
    //


    @Override
    public Long createEvent(String taskName, String queueId, String data, String key2) {
        return createTaskInternal(taskName, Model.CHANNEL_QUEUE, data, nvle(queueId), key2, null, 0, TedStatus.SLEEP, null);
    }

    @Override
    public TaskRec eventQueueMakeFirst(String queueId) {
        String sql = "update $tedTask set status = 'NEW', nextts = now() where system = '$sys'" +
            " and key1 = ? and status = 'SLEEP' and channel = 'TedEQ'" +
            " and taskid = (select min(taskid) from $tedTask t2 " +
            "   where system = '$sys' and channel = 'TedEQ' and t2.key1 = ?" +
            "   and status = 'SLEEP')" +
            " and not exists (select taskid from $tedTask t3" +
            "   where system = '$sys' and channel = 'TedEQ' and t3.key1 = ?" +
            "   and status in ('NEW', 'RETRY', 'WORK', 'ERROR'))" +
            " returning $tedTask.*";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        try {
            List<TaskRec> recs = selectData("event_make_first", sql, TaskRec.class, asList(
                sqlParam(queueId, JetJdbcParamType.STRING),
                sqlParam(queueId, JetJdbcParamType.STRING),
                sqlParam(queueId, JetJdbcParamType.STRING)
            ));
            if (recs.size() != 1) {
                //logger.debug("logid='{}' cannot update, does exists taskid={} ?", taskId);
                return null;
            }
            return recs.get(0);
        } catch (TedSqlDuplicateException e) {
            return null;
        }
    }

    @Override
    public List<TaskRec> eventQueueGetTail(String queueId) {
        String sql = "select * from $tedTask where key1 = ?"
            + " and status = 'SLEEP'"
            + " and channel = '$channel' and system = '$sys'"
            + " order by taskid"
            + " for update nowait"
            + " limit 100";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$channel", Model.CHANNEL_QUEUE);
        List<TaskRec> recs;
        try {
            recs = selectData("queue_tail", sql, TaskRec.class, asList(
                sqlParam(queueId, JetJdbcParamType.STRING)
            ));
        } catch (Exception e) {
            logger.info("select queue_tail got exception: {}", e.getMessage());
            return Collections.emptyList();
        }

        return recs;
    }

    @Override
    public List<TaskRec> getLastNotifications(Date fromTs) {
        String sql = "select * from $tedTask where "
            + " system = '$sys' and channel = '$channel'"
            + " and nextts >= ?"
            + " limit 1000";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$channel", Model.CHANNEL_NOTIFY);
        List<TaskRec> recs = selectData("notif_get", sql, TaskRec.class, asList(
            sqlParam(fromTs, JetJdbcParamType.TIMESTAMP)
        ));
        return recs;
    }

    @Override
    public void cleanupNotifications(Date tillTs) {
        String sql = "update $tedTask "
            + " set nextts = null, status = 'DONE', finishts = now()"
            + " where system = '$sys'"
            + " and taskid in (select taskid from $tedTask"
            + "  where system = '$sys' and channel = '$channel' and status = 'NEW'"
            + "  and nextts < ?"
            + "  limit 1000"
            + "  for update skip locked"
            + " )";

        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$channel", Model.CHANNEL_NOTIFY);
        execute("notif_clean", sql, asList(
            sqlParam(tillTs, JetJdbcParamType.TIMESTAMP)
        ));
    }

    @Override
    public void runInTx(Runnable runnable) {
        Connection connection;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            logger.error("Failed to get DB connection: " + e.getMessage());
            throw new TedSqlException("Cannot get DB connection", e);
        }

        Savepoint savepoint = null;
        Boolean autocommit = null;
        String txLogId = "x";
        try {
            autocommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            savepoint = connection.setSavepoint("runInTx");
            txLogId = Integer.toHexString(savepoint.hashCode());
            logger.debug("[B] before start transaction {}", txLogId);

            runnable.run();

            return ;
        } catch (Throwable e) {
            try {
                if (savepoint != null)
                    connection.rollback(savepoint);
                else
                    connection.rollback();
            } catch (Exception rollbackException) {
                logger.warn("Exception while rollbacking", rollbackException);
            }
            throw new RuntimeException(e);
        }
        finally {
            try {
                if (autocommit != null && autocommit != connection.getAutoCommit()) {
                    connection.setAutoCommit(autocommit);
                }
            } catch (SQLException e) {
                logger.warn("Exception while setting back autoCommit mode", e);
            }
            try { if (connection != null) connection.close(); } catch (Exception e) {logger.error("Cannot close connection", e);};
            logger.debug("[E] before start transaction {}", txLogId);
        }
    }

    @Override
    public boolean maintenanceRebuildIndex() {
        String sql = "reindex index " + schemaPrefix() + INDEX_QUICKCHK;
        try {
            execute("reindex_quickchk", sql, Collections.emptyList());
            return true;
        } catch (Exception e) {
            logger.warn("Cannot rebuild ix_tedtask_quickchk index: {}", e.getMessage());
            logger.debug("Exception while executing '"+ sql +"'", e);
            return false;
        }
    }


    //
    // private
    //

    private void executePgCopy(Connection connection, List<TaskParam> taskParams) throws SQLException {
        String sql = "COPY $tedTask (taskId, system, name, channel, status, key1, key2, batchId, data)" +
            " FROM STDIN " +
            " WITH (FORMAT text, DELIMITER '\t', ENCODING 'UTF-8')";

        sql = sql.replace("$tedTask", fullTableName);

        StringBuilder stringBuilder = new StringBuilder(512);
        for (TaskParam task : taskParams) {
            String str = task.taskId
                + "\t" + thisSystem
                + "\t" + task.name
                + "\t" + task.channel
                + "\t" + TedStatus.NEW
                + "\t" + formatStr(task.key1)
                + "\t" + formatStr(task.key2)
                + "\t" + task.batchId
                + "\t" + formatStr(task.data)
                + "\n";
            stringBuilder.append(str);
        }
        //System.out.println(stringBuilder.toString());
        CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
        try {
            copyManager.copyIn(sql, new StringReader(stringBuilder.toString()));
        } catch (IOException e) {
            throw new SQLException("Unable to execute COPY operation", e);
        }
    }

    private static String formatStr(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\").replace("\t", "\\t").replace("\r", "\\r").replace("\n", "\\n");
    }

    private static class ResSeqVal {
        Long seqval;
    }

    /* postgres only */
    protected ArrayList<Long> getSequencePortion(String seqName, int number) {
        if (number < 1 || number > 100000)
            throw new IllegalArgumentException("Invalid requested sequence count: " + number);
        if (schema != null)
            seqName = schema + "." + seqName;
        String sql = "select nextval('" + seqName + "') as seqval from generate_series(1," + number + ")";
        List<ResSeqVal> seqVals = selectData("seq_portion", sql, ResSeqVal.class, Collections.emptyList());
        ArrayList<Long> result = new ArrayList<>();
        for (ResSeqVal item : seqVals) {
            result.add(item.seqval);
        }
        return result;
    }

    // use 1 call for postgres, instead of 2 (sequence and insert)
    @Override
    protected long createTaskInternal(String name, String channel, String data, String key1, String key2, Long batchId, int postponeSec, TedStatus status, Connection conn) {
        String sqlLogId = "create_task";
        if (status == null)
            status = TedStatus.NEW;
        String nextts = (status == TedStatus.NEW ? dbType.sql().now() + " + " + dbType.sql().intervalSeconds(postponeSec) : "null");

        String sql = " insert into $tedTask (taskId, system, name, channel, bno, status, createTs, nextTs, retries, data, key1, key2, batchId)" +
            " values($nextTaskId, '$sys', ?, ?, null, '$status', $now, $nextts, 0, ?, ?, ?, ?)" +
            " returning taskId";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$nextTaskId", dbType.sql().sequenceSql(schemaPrefix() + "SEQ_TEDTASK_ID"));
        sql = sql.replace("$now", dbType.sql().now());
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$nextts", nextts);
        sql = sql.replace("$status", status.toString());
        final String sqlFinal = sql;

        List<TaskIdRes> resList = smartRunWithLog(conn, sqlLogId,
            conn1 -> JdbcSelectTedImpl.selectData(conn1, sqlFinal, TaskIdRes.class, asList(
                sqlParam(name, JetJdbcParamType.STRING),
                sqlParam(channel, JetJdbcParamType.STRING),
                sqlParam(data, JetJdbcParamType.STRING),
                sqlParam(key1, JetJdbcParamType.STRING),
                sqlParam(key2, JetJdbcParamType.STRING),
                sqlParam(batchId, JetJdbcParamType.LONG)
            )));
        Long taskId = resList.get(0).taskid;
        logger.trace("Task {} {} created successfully. ", name, taskId);
        return taskId;

    }

}
