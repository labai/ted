package ted.driver.sys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ted.driver.Ted.TedProcessor;
import ted.driver.Ted.TedProcessorFactory;
import ted.driver.Ted.TedStatus;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.driver.sys.Trash.TedMetricsEvents;

import java.lang.reflect.InvocationTargetException;

import static ted.driver.sys.TestUtils.sleepMs;

public class TestTedProcessors {
    private final static Logger logger = LoggerFactory.getLogger(TestTedProcessors.class);

    public static class Test09RandProcessor implements TedProcessor {
        private static int inum = 0;
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process " + task);
            sleepMs(20);
            if (++inum % 10 == 0)
                return TedResult.retry("error-" + inum);
            return TedResult.done();
        }
    }

    public static class TestProcessorFailAfterNDone implements TedProcessor {
        private final int okCount;
        private final TedResult fail;
        private int inum = 0;

        public TestProcessorFailAfterNDone(int okCount, TedResult fail) {
            this.okCount = okCount;
            this.fail = fail;
        }

        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process " + task);
            sleepMs(5);
            if (++inum > okCount)
                return fail;
            return TedResult.done();
        }

        public int num() { return inum; }
    }


    public static class TestProcessorOk implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process");
            sleepMs(20);
            return TedResult.done();
        }
    }

    public static class TestProcessorOkSleep implements TedProcessor {
        private final int sleepMs;
        public TestProcessorOkSleep(int sleepMs) {
            this.sleepMs = sleepMs;
        }
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process");
            sleepMs(sleepMs);
            return TedResult.done();
        }
    }


    public static class TestProcessorException implements TedProcessor {
        @Override
        public TedResult process(TedTask task)  {
            logger.info(this.getClass().getSimpleName() + " process");
            sleepMs(20);
            throw new RuntimeException("Test runtime exception");
        }
    }

    public static class SingeInstanceFactory implements TedProcessorFactory {
        private TedProcessor tedProcessor;

        public SingeInstanceFactory(TedProcessor tedProcessor) {
            this.tedProcessor = tedProcessor;
        }

        @Override
        public TedProcessor getProcessor(String taskName) {
            return tedProcessor;
        }
    }

    public static TedProcessorFactory forProcessor(TedProcessor processor) {
        return new SingeInstanceFactory(processor);
    }


    public static <T extends TedProcessor> TedProcessorFactory forClass(final Class<T> clazz) {
        return taskName -> {
            try {
                return clazz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException("Class instantiate exception", e);
            } catch (NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException("Class instantiate exception", e);
            }
        };
    }

    public static class OnTaskFinishListener implements TedMetricsEvents {
        public interface OnFinish {
            void onFinishTask(long taskId, TedStatus status);
        }
        private final OnFinish onFinish;

        public OnTaskFinishListener(OnFinish onFinish) {
            this.onFinish = onFinish;
        }

        @Override public void dbCall(String logId, int resultCount, int durationMs) { }
        @Override public void loadTask(long taskId, String taskName, String channel) { }
        @Override public void startTask(long taskId, String taskName, String channel) { }

        @Override
        public void finishTask(long taskId, String taskName, String channel, TedStatus status, int durationMs) {
            onFinish.onFinishTask(taskId, status);
        }
    }


}

