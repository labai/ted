package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.driver.Ted.TedProcessor
import ted.driver.Ted.TedProcessorFactory
import ted.driver.Ted.TedRetryScheduler
import ted.driver.Ted.TedStatus
import ted.driver.TedDriver
import ted.driver.TedResult
import ted.driver.TedTask
import ted.driver.sys.SqlUtils.DbType
import ted.driver.sys.SqlUtils.DbType.HSQLDB
import ted.driver.sys.SqlUtils.DbType.MYSQL
import ted.driver.sys.SqlUtils.DbType.ORACLE
import ted.driver.sys.SqlUtils.DbType.POSTGRES
import ted.scheduler.TedScheduler
import ted.scheduler.TedScheduler.TedSchedulerNextTime
import ted.scheduler.utils.CronExpression
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import javax.sql.DataSource
import kotlin.math.min


/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 */
internal class TedSchedulerImpl(private val tedDriver: TedDriver) {
    private val tedSchdDriverExt: TedSchdDriverExt
    private val maintenanceExecutor: ScheduledExecutorService

    private val schedulerTasks = mutableMapOf<String, SchedulerInfo>()

    // internal for tests
    internal val context: Context

    internal class Context {
        internal lateinit var tedDriver: TedDriver
        internal lateinit var taskRecService: TaskRecService
        internal lateinit var thisSystem: String
        internal lateinit var tableName: String
        internal var schemaName: String? = null
        internal lateinit var dbType: DbType
        internal lateinit var dataSource: DataSource
        internal lateinit var dao: ISchedulerDao
        internal lateinit var primeTaskIdProvider: () -> Long?
        internal fun getFullTableName() = if (schemaName == null) tableName else "$schemaName.$tableName"
    }

    private class SchedulerInfo(
        val name: String,
        val taskId: Long,
        val retryScheduler: TedRetryScheduler
    )

    init {

        this.tedSchdDriverExt = TedSchdDriverExt(tedDriver)

        this.context = Context()
        this.context.tedDriver = tedDriver
        this.context.thisSystem = tedSchdDriverExt.systemId()
        this.context.tableName = tedSchdDriverExt.tableName()
        this.context.schemaName = tedSchdDriverExt.schemaName()
        this.context.dataSource = tedSchdDriverExt.dataSource()
        this.context.dbType = tedSchdDriverExt.dbType()
        this.context.primeTaskIdProvider = { tedSchdDriverExt.primeTaskId() }
        this.context.taskRecService = TaskRecService(context)


        this.context.dao = when (context.dbType) {
            ORACLE -> DaoOracle(context)
            POSTGRES -> DaoPostgres(context)
            MYSQL -> DaoMysql(context)
            HSQLDB -> DaoHsqldb(context)
        }

        maintenanceExecutor = createSchedulerExecutor("TedSchd-")
        maintenanceExecutor.scheduleAtFixedRate({
            try {
                checkForErrorStatus()
            } catch (e: Throwable) {
                logger.error("Error while executing scheduler maintenance tasks", e)
            }
        }, 30, 180, TimeUnit.SECONDS)
    }

    fun shutdown() {
        maintenanceExecutor.shutdown()
    }


    fun registerScheduler(taskName: String, data: String?, processorFactory: TedProcessorFactory, retryScheduler: TedRetryScheduler): Long {
        val disabled = tedSchdDriverExt.getPropertyValue("ted.schedulerTask.${taskName}.disabled")?.yesToBoolean() ?: false
        val cron = tedSchdDriverExt.getPropertyValue("ted.schedulerTask.${taskName}.cron")

        if (disabled) {
            logger.warn("scheduled task $taskName is disabled")
            registerDisabledScheduler(taskName)
        } else if (!cron.isNullOrBlank()) {
            logger.info("Using cron from properties (ted.schedulerTask.${taskName}.cron=$cron)")
            tedDriver.registerTaskConfig(taskName, processorFactory, CronRetry(cron))
        } else {
            tedDriver.registerTaskConfig(taskName, processorFactory, retryScheduler)
        }

        // create task is not exists
        val postponeSec = getPostponeSec(retryScheduler)
        val taskId = context.taskRecService.createUniqueTask(taskName, data, "", null, postponeSec) ?: throw IllegalStateException("taskId == null for task $taskName")

        schedulerTasks[taskName] = SchedulerInfo(taskName, taskId, retryScheduler)

        return taskId
    }


    private fun registerDisabledScheduler(taskName: String) {
        // always postpone task for 10 minutes. Once disabled, will require to restart to enable
        val disabledTaskProcessor = TedProcessorFactory {
            TedProcessor {
                logger.debug("scheduled task $taskName is disabled, skipping")
                TedResult.retry("disabled")
            }
        }
        tedDriver.registerTaskConfig(taskName, disabledTaskProcessor, PeriodicRetry(10, TimeUnit.MINUTES))
    }


    fun checkForErrorStatus() {
        val schdTaskIds = schedulerTasks.values.map { it.taskId }
        val badTaskIds = context.dao.checkForErrorStatus(schdTaskIds)
        for (taskId in badTaskIds) {
            val schInfo = schedulerTasks.values.firstOrNull { it.taskId == taskId }
            logger.warn("Restore schedule task {} {} from ERROR to RETRY", taskId, schInfo?.name ?: "null")

            val task = tedDriver.getTask(taskId)

            val postponeSec = if (schInfo == null) 0 else getPostponeSec(schInfo.retryScheduler)
            context.dao.restoreFromError(taskId, task.name, postponeSec)
        }

    }

    // wrap original processor and on error return retry
    internal class SchedulerProcessorFactory(private val origTedProcessorFactory: TedProcessorFactory) : TedProcessorFactory {

        override fun getProcessor(taskName: String): TedProcessor {
            return SchedulerProcessor(origTedProcessorFactory.getProcessor(taskName))
        }
    }

    private class SchedulerProcessor(private val origTedProcessor: TedProcessor) : TedProcessor {

        override fun process(task: TedTask): TedResult {
            val result: TedResult
            try {
                result = origTedProcessor.process(task) ?: TedResult.error("null returned as result")
            } catch (e: Throwable) {
                logger.warn("Got exception, but will retry anyway: {}", e.message)
                return TedResult.retry(e.message)
            }

            if (result.status() == TedStatus.ERROR) {
                logger.warn("Got error, but will retry anyway: {}", result.message())
            }
            return TedResult.retry(result.message())
        }
    }

    // use cron expression
    internal class CronRetry(cron: String) : TedRetryScheduler {
        private val cronExpr: CronExpression = CronExpression(cron)

        override fun getNextRetryTime(task: TedTask?, retryNumber: Int, startTime: Date): Date {
            val ztm = ZonedDateTime.ofInstant(startTime.toInstant(), ZoneId.systemDefault())
            return Date.from(cronExpr.nextTimeAfter(ztm).toInstant())
        }
    }

    internal class CustomRetry(private val nextTimeFn: TedSchedulerNextTime) : TedRetryScheduler {

        override fun getNextRetryTime(task: TedTask?, retryNumber: Int, startTime: Date): Date {
            return nextTimeFn.getNextTime(startTime)
        }
    }

    internal class PeriodicRetry(period: Int, timeUnit: TimeUnit) : TedRetryScheduler {
        private val periodMs: Long = TimeUnit.MILLISECONDS.convert(period.toLong(), timeUnit)

        override fun getNextRetryTime(task: TedTask?, retryNumber: Int, startTime: Date?): Date {
            return Date((startTime?.time ?: System.currentTimeMillis()) + periodMs)
        }
    }

    private class SingeInstanceFactory(private val tedProcessor: TedProcessor) : TedProcessorFactory {
        override fun getProcessor(taskName: String): TedProcessor {
            return tedProcessor
        }
    }


    object Factory {
        @JvmStatic
        fun single(runnable: Runnable): TedProcessorFactory {
            return SchedulerProcessorFactory(SingeInstanceFactory(TedProcessor {
                runnable.run()
                TedResult.done()
            }))
        }

        @JvmStatic
        fun single(tedProcessor: TedProcessor): TedProcessorFactory {
            return SchedulerProcessorFactory(SingeInstanceFactory(tedProcessor))
        }

    }

    private fun createSchedulerExecutor(prefix: String): ScheduledExecutorService {
        val threadFactory = object : ThreadFactory {
            private var counter = 0
            override fun newThread(runnable: Runnable): Thread {
                return Thread(runnable, prefix + ++counter)
            }
        }
        return Executors.newSingleThreadScheduledExecutor(threadFactory)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(TedScheduler::class.java)

        private fun getPostponeSec(retryScheduler: TedRetryScheduler): Int {
            val startFrom = retryScheduler.getNextRetryTime(null, 1, Date())
            var postponeSec = 0
            if (startFrom != null) {
                postponeSec = min(0L, startFrom.time - System.currentTimeMillis() / 1000).toInt()
            }
            return postponeSec
        }
    }


    private fun String.yesToBoolean(): Boolean = this.equals("yes", ignoreCase = true) || this.equals("true", ignoreCase = true)
}
