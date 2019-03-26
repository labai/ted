package ted.scheduler


import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import ted.driver.Ted
import ted.driver.Ted.TedProcessor
import ted.driver.Ted.TedProcessorFactory
import ted.driver.TedResult
import ted.driver.TedTask
import ted.scheduler.impl.TedSchedulerImpl
import ted.scheduler.impl.TedSchedulerImpl.PeriodicRetry
import ted.scheduler.impl.TedSchedulerImpl.SchedulerProcessorFactory
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class F02FunctionTest {
    private val logger = LoggerFactory.getLogger(F02FunctionTest::class.java)

    val fakeTask = TedTask(1001L, "TEST1", null, null, null);


    @Test
    internal fun `test01 schedulerProcessor must return retry`() {

        val fact = TedProcessorFactory {
            return@TedProcessorFactory TedProcessor {
                TedResult.error()
            }
        }

        val schFact = SchedulerProcessorFactory(fact);

        val res = schFact.getProcessor("x").process(fakeTask)

        assertEquals(Ted.TedStatus.RETRY, res.status)
    }

    @Test
    internal fun `test periodicRetry`() {
        val pr = PeriodicRetry(50, TimeUnit.MILLISECONDS);
        val startTs = 1000_000L;
        val nextTs = pr.getNextRetryTime(fakeTask, 22, Date(startTs))

        assertEquals(1000_050, nextTs.time)

    }
}