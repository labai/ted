package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.sys.JdbcSelectTed.JetJdbcParamType;
import ted.driver.sys.JdbcSelectTed.SqlParam;
import ted.driver.sys.Model.TaskRec;
import ted.driver.sys.SqlUtils.DbType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static ted.driver.sys.JdbcSelectTed.sqlParam;

/**
 * @author Augustus
 *         created on 2016.09.13
 *
 * for TED internal usage only!!!
 *
 * rewrite functions for faster execution using Oracle feature - block statements
 *
 */
class TedDaoOracle extends TedDaoAbstract {
    private static final Logger logger = LoggerFactory.getLogger(TedDaoAbstract.class);

    public TedDaoOracle(String system, DataSource dataSource, Stats stats, String schema, String tableName) {
        super(system, dataSource, DbType.ORACLE, stats, schema, tableName);
    }

    @Override
    protected long createTaskInternal(String name, String channel, String data, String key1, String key2, Long batchId, int postponeSec, TedStatus status, Connection conn) {
        String sqlLogId = "create_task0";
        Long nextId = getSequenceNextValue("SEQ_TEDTASK_ID");
        if (status == null)
            status = TedStatus.NEW;
        String nextts = (status == TedStatus.NEW ? dbType.sql().now() + " + " + dbType.sql().intervalSeconds(postponeSec) : "null");

        String sql = " insert into $tedTask (taskId, system, name, channel, bno, status, createTs, nextTs, retries, data, key1, key2, batchId)" +
            " values(?, '$sys', ?, ?, null, '$status', $now, $nextts, 0, ?, ?, ?, ?)";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("$now", dbType.sql().now());
        sql = sql.replace("$sys", thisSystem);
        sql = sql.replace("$nextts", nextts);
        sql = sql.replace("$status", status.toString());

        execute(conn, sqlLogId, sql, asList(
            sqlParam(nextId, JetJdbcParamType.LONG),
            sqlParam(name, JetJdbcParamType.STRING),
            sqlParam(channel, JetJdbcParamType.STRING),
            sqlParam(data, JetJdbcParamType.STRING),
            sqlParam(key1, JetJdbcParamType.STRING),
            sqlParam(key2, JetJdbcParamType.STRING),
            sqlParam(batchId, JetJdbcParamType.LONG)
        ));
        logger.trace("Task {} {} created successfully. ", name, nextId);
        return nextId;
    }

    @Override
    public List<TaskRec> reserveTaskPortion(Map<String, Integer> channelSizes){
        String channelsParam = "";
        for (String channel : channelSizes.keySet()) {
            int cnt = channelSizes.get(channel);
            if (cnt == 0) continue;
            channelsParam = ("".equals(channelsParam) ? "" : channelsParam + ",") + channel + ":" + cnt;
        }

        String oraProc = "get_next_portion_all(or)";
        String sql = ""
            + " declare"
            + "   v_bno number;"
            + "   now timestamp(3) := systimestamp;"
            + "	  p_sys $tedTask.system%type := :p_sys;"
            + "   p_pairs varchar(500) := :p_pairs; "
            + "   v_taskid number;"

            + "   cursor c2 (vchan in varchar, vcnt in integer) is"
            + "     select taskid from $tedTask"
            + "     where status in ('NEW', 'RETRY') and system = p_sys and channel = vchan"
            + "       and nextTs < now "
            + "       and rownum <= vcnt"
            + "       for update skip locked;"

            + " begin"
            + 	" v_bno := ${schemaPrefix}SEQ_TEDTASK_BNO.nextval;"

            // iterate through 'channel:count,channel2:count2' pairs
            + 	" for cur in (select substr(str, 1, instr(str, ':') - 1) chan, to_number(substr(str, instr(str, ':') + 1)) cnt"
            + 	"    	from (select trim(regexp_substr(p_pairs, '[^,]+', 1, level)) str from dual connect by instr(p_pairs, ',', 1, level - 1) > 0)"
            + 		" 		) loop"

            // take NEW and RETRY tasks of channel cur.chan, maximum cur.cnt
            +		" open c2(cur.chan, cur.cnt); "
            + 		" loop "
            + 		" 	fetch c2 into v_taskid; "
            + 		" 	exit when c2%notfound; "
            +		"   update $tedTask set status = 'WORK', bno = v_bno, startTs = now, nextTs = null"
            + 		"      where current of c2;"
            + 		" end loop;"
            + 		" close c2;"

            +	" end loop;"
            + 	" commit;"

            // return all taken tasks
            + 	" open :o_rs for select * from $tedTask where bno = v_bno;"
            + " end;";
        sql = sql.replace("$tedTask", fullTableName);
        sql = sql.replace("${schemaPrefix}", schemaPrefix());
        List<TaskRec> tasks = selectFromBlock(oraProc, sql, TaskRec.class, asList(
            JdbcSelectTed.sqlParam("p_sys", thisSystem, JetJdbcParamType.STRING),
            JdbcSelectTed.sqlParam("p_pairs", channelsParam, JetJdbcParamType.STRING),
            JdbcSelectTed.sqlParam("o_rs", "", JetJdbcParamType.CURSOR)
        ));
        return tasks;
    }

    //
    // wrapper
    //
    <T> List<T> selectFromBlock(String sqlLogId, final String sql, final Class<T> clazz, final List<SqlParam> params) {
        return runInConnWithLog(sqlLogId,
            conn -> JdbcSelectTedImpl.executeOraBlock(conn, sql, clazz, params));
    }

}

