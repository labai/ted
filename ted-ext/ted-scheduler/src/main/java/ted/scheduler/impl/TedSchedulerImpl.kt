package ted.scheduler.impl

import org.slf4j.LoggerFactory
import ted.driver.Ted.*
import ted.driver.TedDriver
import ted.driver.TedResult
import ted.driver.TedTask
import ted.driver.sys._TedSchdDriverExt
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
import kotlin.streams.toList

/**
 * @author Augustus
 * created on 2018.08.16
 *
 * for TED internal usage only!!!
 */
internal class TedSchedulerImpl(private val tedDriver: TedDriver) {
    private val tedSchdDriverExt: _TedSchdDriverExt
    private val dataSource: DataSource
    private val dao: DaoPostgres
    private val maintenanceExecutor: ScheduledExecutorService

    private val schedulerTasks = mutableMapOf<String, SchedulerInfo>()


    private class SchedulerInfo(
            internal val name: String,
            internal val taskId: Long?,
            internal val retryScheduler: TedRetryScheduler)

    init {
        this.tedSchdDriverExt = _TedSchdDriverExt(tedDriver)
        this.dataSource = tedSchdDriverExt.dataSource()
        this.dao = DaoPostgres(dataSource, tedSchdDriverExt.systemId())

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
        if (! tedSchdDriverExt.isPrimeEnabled)
            throw IllegalStateException("Prime-instance functionality must be enabled!")

        tedDriver.registerTaskConfig(taskName, processorFactory, retryScheduler)

        // create task is not exists
        val postponeSec = getPostponeSec(retryScheduler)
        val taskId = createUniqueTask(taskName, data, "", null, postponeSec) ?: throw IllegalStateException("taskId == null for task $taskName")

        schedulerTasks[taskName] = SchedulerInfo(taskName, taskId, retryScheduler)

        return taskId
    }

    fun checkForErrorStatus() {
        val schdTaskIds = schedulerTasks.values.stream().map<Long> { sch -> sch.taskId }.toList()
        val badTaskIds = dao.checkForErrorStatus(schdTaskIds)
        for (taskId in badTaskIds) {
            val schInfo = schedulerTasks.values.firstOrNull { sch -> sch.taskId == taskId }
            logger.warn("Restore schedule task {} {} from ERROR to RETRY", taskId, schInfo?.name ?: "null")

            val task = tedDriver.getTask(taskId)

            val postponeSec = if (schInfo == null) 0 else getPostponeSec(schInfo.retryScheduler)
            dao.restoreFromError(taskId, task.name, postponeSec)
        }

    }


    /* creates task only if does not exists (task + activeStatus).
	   While there are not 100% guarantee, but will try to ensure, that 2 processes will not create same task twice (using this method).
	   If task with ERROR status will be found, then it will be converted to RETRY
	*/
    private fun createUniqueTask(name: String, data: String?, key1: String?, key2: String?, postponeSec: Int): Long? {
        val primeTaskId = tedSchdDriverExt.primeTaskId() ?: throw java.lang.IllegalStateException("primeTaskId not found")

        return dao.execWithLockedPrimeTaskId(dataSource, primeTaskId) fn@ {
            val taskIds = dao.get2ActiveTasks(name, true)
            if (taskIds.size > 1)
                throw IllegalStateException("Exists more than one $name active scheduler task (statuses NEW, RETRY, WORK or ERROR) $taskIds, skipping")
            if (taskIds.size == 1) { // exists exactly one task
                val taskId = taskIds[0]
                val tedTask = tedDriver.getTask(taskId)
                if (tedTask!!.status == TedStatus.ERROR) {
                    logger.info("Restore scheduler task {} {} from error", name, taskId)
                    dao.restoreFromError(taskId, name, postponeSec)
                    return@fn taskId
                }
                logger.debug("Exists scheduler task {} with active status (NEW, RETRY or WORK)", name)
                return@fn taskIds[0]
            }
            logger.debug("No active scheduler tasks {} exists, will create new", name)
            tedDriver.createTaskPostponed(name, data, key1, key2, postponeSec)
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

        override fun getNextRetryTime(task: TedTask, retryNumber: Int, startTime: Date): Date {
            return nextTimeFn.getNextTime(startTime)
        }
    }

    internal class PeriodicRetry(period: Int, timeUnit: TimeUnit) : TedRetryScheduler {
        private val periodMs: Long = TimeUnit.MILLISECONDS.convert(period.toLong(), timeUnit)

        override fun getNextRetryTime(task: TedTask, retryNumber: Int, startTime: Date?): Date {
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
            return SchedulerProcessorFactory(SingeInstanceFactory(TedProcessor() { _ ->
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
                postponeSec = Math.min(0L, startFrom.time - System.currentTimeMillis() / 1000).toInt()
            }
            return postponeSec
        }
    }


}
