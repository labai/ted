package ted.spring.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Augustus
 *         created on 2019.12.27
 *
 *  Annotation on dataSource beans to mark as default for Ted driver
 *  in case when few datasources exists.
 *
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface TedDataSource {
}
