package ted.spring.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import ted.driver.TedDriver;
import ted.driver.TedResult;
import ted.driver.TedTask;
import ted.scheduler.TedScheduler;
import ted.spring.annotation.TedSchedulerProcessor;
import ted.spring.annotation.TedTaskProcessor;
import ted.spring.exception.TedRetryException;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 * for TED internal usage only!!!
 *
 */
class TaskRegistrar {
	private static final Logger logger = LoggerFactory.getLogger(TaskRegistrar.class);
	private static final Class<? extends Throwable> DEFAULT_RETRYABLE_EXCEPTION = TedRetryException.class;

	private ApplicationContext applicationContext;
	private TedScheduler tedScheduler;
	private TedDriver tedDriver;

	public TaskRegistrar(ApplicationContext applicationContext, TedDriver tedDriver, TedScheduler tedScheduler) {
		this.applicationContext = applicationContext;
		this.tedDriver = tedDriver;
		this.tedScheduler = tedScheduler;
	}

	void registerAnnotatedTasks() {
		// do one search for all ted annotation
		Map<Class<? extends Annotation>, List<AnnMethod>> annMethods = getTedAnnotatedMethods();

		List<AnnMethod> tasks = annMethods.get(TedTaskProcessor.class);
		registerAnnotatedTasks(tasks);

		List<AnnMethod> schedulers = annMethods.get(TedSchedulerProcessor.class);
		registerAnnotatedSchedulers(schedulers);
	}

	private void registerAnnotatedTasks(List<AnnMethod> tasks) {
		for (AnnMethod annMethod : tasks) {
			TedTaskProcessor ttp = (TedTaskProcessor)annMethod.annotation;
			String taskName = ttp.name();
			logger.debug("Register task {} in bean {} method {}", taskName, annMethod.beanName, annMethod.method.getName());
			tedDriver.registerTaskConfig(taskName, s -> task -> {
				return invokeProcessor(annMethod, task, ttp.retryException());
			});
		}
	}

	private void registerAnnotatedSchedulers(List<AnnMethod> schedulers) {
		for (AnnMethod annMethod : schedulers) {
			TedSchedulerProcessor tsp = (TedSchedulerProcessor)annMethod.annotation;
			String taskName = tsp.name();
			logger.debug("Register scheduler task {} with cron '{}' in bean {} method {}", taskName, tsp.cron(),annMethod.beanName, annMethod.method.getName());
			tedScheduler.builder()
					.name(taskName)
					.processor(task -> invokeProcessor(annMethod, task,null))
					.scheduleCron(tsp.cron())
					.register();
		}
	}

	private TedResult invokeProcessor(AnnMethod annMethod, TedTask task, Class<? extends Throwable>[] retryExceptions) {
		Object[] args;
		try {
			args = fillArgs(annMethod.method, task);
		} catch (Exception e) {
			logger.error("Invalid TedTask arguments", e);
			return TedResult.error(e.getMessage());
		}

		try {
			Object res = annMethod.method.invoke(annMethod.bean, args);
			return resolveTedResult(res, annMethod.method.getReturnType());
		} catch (IllegalAccessException e) {
			logger.error("Can't execute task", e);
			return TedResult.error("CantExecute:" + e.getMessage());
		}
		catch (InvocationTargetException e) {
			logger.debug("Exception while executing task: {}", (e.getMessage() != null ? e.getMessage() : (e.getCause() == null ? "null" : e.getCause().getMessage())));
			logger.trace("Exception while executing task", e);
			if (e.getCause() != null)
				return resolveErrorResult(e.getCause(), retryExceptions);
			return TedResult.error("CantExecute:" + e.getMessage());
		} catch (Throwable e) {
			logger.debug("Can't execute task", e);
			return resolveErrorResult(e, retryExceptions);
		}

	}

	private Object[] fillArgs(Method method, ted.driver.TedTask task) {
		Object[] result = new Object[method.getParameterTypes().length];
		for (int i = 0; i < method.getParameterTypes().length; i++) {
			Class<?> argCls = method.getParameterTypes()[i];
			if (argCls.equals(ted.driver.TedTask.class)) {
				result[i] = task;
				continue;
			}
			throw new IllegalStateException("Invalid TedTask argument name. Allowed to use types: TedTask");
		}
		return result;
	}

	private TedResult resolveErrorResult(Throwable e, Class<? extends Throwable>[] retryExceptions) {
		if (DEFAULT_RETRYABLE_EXCEPTION.isAssignableFrom(e.getClass()))
			return TedResult.retry(e.getMessage());
		if (retryExceptions != null) {
			for (Class<? extends Throwable> rexcls : retryExceptions) {
				if (rexcls.isAssignableFrom(e.getClass()))
					return TedResult.retry(e.getMessage());
			}
		}
		return TedResult.error(e.getMessage());
	}

	private TedResult resolveTedResult(Object ret, Class<?> returnType) {
		if (void.class.equals(returnType) || Void.class.equals(returnType))
			return TedResult.done("");
		if (ret == null)
			return TedResult.done("null");
		if (ret instanceof TedResult)
			return (TedResult) ret;
		if (ret instanceof String)
			return TedResult.done((String)ret);
		return TedResult.done("[Invalid return type]");
	}

	private Map<Class<? extends Annotation>, List<AnnMethod>> getTedAnnotatedMethods() {
		Map<Class<? extends Annotation>, List<AnnMethod>> result = new HashMap<>();
		result.put(TedTaskProcessor.class, new ArrayList<>());
		result.put(TedSchedulerProcessor.class, new ArrayList<>());
		for (String beanName : applicationContext.getBeanDefinitionNames()) {
			Object bean;
			try {
				bean = applicationContext.getBean(beanName);
			} catch (BeansException e) {
				logger.debug("Skip bean '{}' ({})", beanName, e.getMessage());
				continue;
			}
			Class<?> objClz = resolveRealClass(bean);

			for (Method m : objClz.getDeclaredMethods()) {
				Annotation a = AnnotationUtils.findAnnotation(m, TedTaskProcessor.class);
				if (a != null) {
					AnnMethod annMethod = new AnnMethod(TedTaskProcessor.class, a, bean, beanName, objClz, m);
					result.get(TedTaskProcessor.class).add(annMethod);
				}
				a = AnnotationUtils.findAnnotation(m, TedSchedulerProcessor.class);
				if (a != null) {
					AnnMethod annMethod = new AnnMethod(TedSchedulerProcessor.class, a, bean, beanName, objClz, m);
					result.get(TedSchedulerProcessor.class).add(annMethod);
				}
			}
		}
		return result;
	}


	private static Class<?> resolveRealClass(Object obj) {
		// if want to check proxy
		// logger.info("cglib={} aop={} jdk={} cglib2={} cglib3={}", org.springframework.cglib.proxy.Proxy.isProxyClass(obj.getClass()), AopUtils.isAopProxy(obj), AopUtils.isJdkDynamicProxy(obj), AopUtils.isCglibProxy(obj), ClassUtils.isCglibProxy(obj));

		Class<?> clazz = AopUtils.getTargetClass(obj);
		if (isCglibProxyClassName(clazz.getName())) {
			clazz = ClassUtils.getUserClass(clazz);
		}
		return clazz;
	}

	//
	// ClassUtils.isCglibProxy(obj) is deprecated, but I found it is only one which detects
	// Spring CGLIB proxies (annotated with @Configuration, SpringBootApplication).
	// In AopUtils.isAopProxy() they are not recognized as instanceof SpringProxy
	//
	private static boolean isCglibProxyClassName(@Nullable String className) {
		return (className != null && className.contains(ClassUtils.CGLIB_CLASS_SEPARATOR));
	}

	// annotated methods
	private static class AnnMethod {
		final Class annClass;
		final Annotation annotation;
		final Object bean;
		final String beanName;
		final Class<?> realClass;
		final Method method;
		AnnMethod(Class annClass, Annotation annotation, Object bean, String beanName, Class<?> realClass, Method method) {
			this.annClass = annClass;
			this.annotation = annotation;
			this.bean = bean;
			this.beanName = beanName;
			this.realClass = realClass;
			this.method = method;
		}
	}

}
