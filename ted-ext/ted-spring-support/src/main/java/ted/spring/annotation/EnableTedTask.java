package ted.spring.annotation;

import org.springframework.context.annotation.Import;
import ted.spring.conf.TedTaskConfigurationSelector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 *  Annotation on @Configuration classes to enable TedTasks (TedDriver)
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({TedTaskConfigurationSelector.class})
public @interface EnableTedTask {
}