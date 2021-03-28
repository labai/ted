package ted.spring.annotation;

import ted.spring.exception.TedRetryException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 *  Annotation on bean method
 *  to mark method as task processor
 *
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface TedTaskProcessor {

	/**
	 * Task name, mandatory
	 */
	String name();

	/**
	 * list of exception, on which will set RETRY instead of ERROR
	 */
	Class<? extends Throwable>[] retryException() default TedRetryException.class;
}

