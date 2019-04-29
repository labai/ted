package ted.scheduler


import org.junit.Assert.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import ted.driver.Ted.*
import ted.driver.TedResult
import ted.driver.TedTask
import ted.scheduler.impl.TedSchedulerImpl.PeriodicRetry
import ted.scheduler.impl.TedSchedulerImpl.SchedulerProcessorFactory
import ted.scheduler.utils.CronExpression
import java.time.*
import java.util.*
import java.util.concurrent.TimeUnit

class F02FunctionTest {
    private val logger = LoggerFactory.getLogger(F02FunctionTest::class.java)

    private val fakeTask = object: TedTask {
        override fun getTaskId() = 1001L
        override fun getKey1() = ""
        override fun getKey2() = ""
        override fun getData() = ""
        override fun getRetries() = 0
        override fun getCreateTs() = null
        override fun getStartTs() = null
        override fun getBatchId() = null
        override fun getStatus() = null
        override fun getName() = "TEST1"
        override fun isNew() = true
        override fun isRetry() = false
        override fun isAfterTimeout() = false
    }

    private fun testRetryAfterError(proc : () -> TedResult?) {
        val fact = TedProcessorFactory {
            return@TedProcessorFactory TedProcessor {
                proc()
            }
        }

        val schFact = SchedulerProcessorFactory(fact);

        val res = schFact.getProcessor("x").process(fakeTask)

        assertEquals(TedStatus.RETRY, res.status())

    }

    @Test
    internal fun `schedulerProcessor must return retry on error`() {

        testRetryAfterError { TedResult.error() }

        testRetryAfterError { throw RuntimeException("runtime exception") }

        testRetryAfterError { null }

        testRetryAfterError { throw Error("error") }
    }

    @Test
    internal fun `test periodicRetry`() {
        val pr = PeriodicRetry(50, TimeUnit.MILLISECONDS)
        val startTs = 1_000_000L;
        val nextTs = pr.getNextRetryTime(fakeTask, 22, Date(startTs))

        assertEquals(1_000_050, nextTs.time)

    }

    @Test
    internal fun `test cronExpression sample`() {
        val cron = CronExpression("2/15 14/19 0 1,11,21 1/2 *")

        val lddt = LocalDate.parse("2019-11-08");
        val ldtm = LocalTime.parse("10:11:12");
        var zdt = ZonedDateTime.of(lddt, ldtm, ZoneId.systemDefault());

        zdt = cron.nextTimeAfter(zdt);
        assertEquals(LocalDateTime.parse("2019-11-11T00:14:02"), zdt.toLocalDateTime())
        zdt = cron.nextTimeAfter(zdt);
        assertEquals(LocalDateTime.parse("2019-11-11T00:14:17"), zdt.toLocalDateTime())
        zdt = cron.nextTimeAfter(zdt);
        assertEquals(LocalDateTime.parse("2019-11-11T00:14:32"), zdt.toLocalDateTime())
    }
}