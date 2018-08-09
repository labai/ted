package labai.ted.sys;

import labai.ted.Ted.TedProcessor;
import labai.ted.Ted.TedProcessorFactory;
import labai.ted.TedResult;
import labai.ted.TedTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTedProcessors {
	private final static Logger logger = LoggerFactory.getLogger(TestTedProcessors.class);

	public static class Test09RandProcessor implements TedProcessor {
		private static int inum = 0;
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process " + task);
			TestUtils.sleepMs(20);
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
			TestUtils.sleepMs(5);
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
			TestUtils.sleepMs(20);
			return TedResult.done();
		}
	}

	public static class TestProcessorException implements TedProcessor {
		@Override
		public TedResult process(TedTask task)  {
			logger.info(this.getClass().getSimpleName() + " process");
			TestUtils.sleepMs(20);
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


	public static <T extends TedProcessor> TedProcessorFactory forClass(final Class<T> clazz) {
		return new TedProcessorFactory() {
			@Override
			public TedProcessor getProcessor(String taskName) {
				try {
					return clazz.newInstance();
				} catch (InstantiationException e) {
					throw new RuntimeException("Class instantiate exception", e);
				} catch (IllegalAccessException e) {
					throw new RuntimeException("Class instantiate exception", e);
				}
			}
		};
	}
}
