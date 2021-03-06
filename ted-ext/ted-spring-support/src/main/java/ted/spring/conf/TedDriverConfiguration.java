package ted.spring.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.context.annotation.Role;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.type.AnnotationMetadata;
import ted.driver.TedDriver;
import ted.driver.TedTaskManager;
import ted.driver.task.TedTaskFactory;
import ted.scheduler.TedScheduler;
import ted.spring.annotation.EnableTedTask;
import ted.spring.annotation.TedDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * @author Augustus
 *         created on 2019.12.04
 *
 * for TED internal usage only!!!
 *
 */
@Configuration
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class TedDriverConfiguration implements ImportAware, ApplicationContextAware {
	private static final Logger logger = LoggerFactory.getLogger(TedDriverConfiguration.class);

	private AnnotationAttributes annotationAttributes;

	public TedDriverConfiguration() { }

	@Bean(name = "ted.driver.TedTaskManager")
	TedTaskManager tedTaskManager() {
		return new TedTaskManager();
	}

	@Bean
	TedTaskFactory tedTaskFactory(TedTaskManager taskManager) {
		return taskManager.getTaskFactory();
	}

	@Bean(destroyMethod = "shutdown")
	TedDriver tedDriver(ApplicationContext applicationContext, Environment environment) {
		logger.trace("Create TedDriver");

		DataSource dataSource = resolveDataSource(applicationContext);

		Map<String, String> props = getAllKnownProperties(environment, "ted.");

		if (isEmpty(props.get("ted.systemId"))) {
			props.put("ted.systemId", "default");
			logger.warn("Parameter 'ted.systemId' was not found, value 'default' will be used for systemId");
		}
		Properties properties = new Properties();
		properties.putAll(props);
		TedDriver tedDriver = new TedDriver(dataSource, properties);

		return tedDriver;
	}

	private static DataSource resolveDataSource(ApplicationContext applicationContext) {
		Map<String, DataSource> dataSourceMap = applicationContext.getBeansOfType(DataSource.class);
		if (dataSourceMap.isEmpty())
			throw new IllegalStateException("DataSource is required for TedDriver, but can't be acquired from spring context");
		if (dataSourceMap.size() == 1)
			return dataSourceMap.values().iterator().next();

		// 1) try get with @TedDataSource
		for (Entry<String, DataSource> e : dataSourceMap.entrySet()) {
			TedDataSource an = applicationContext.findAnnotationOnBean(e.getKey(), TedDataSource.class);
			if (an != null)
				return e.getValue();
		}
		// 2) choose @Primary or fail with NoUniqueBeanDefinitionException
		return applicationContext.getBean(DataSource.class);
	}

	@Bean(destroyMethod = "shutdown")
	TedScheduler tedScheduler(TedDriver tedDriver, Environment environment) {
		logger.trace("Create TedScheduler");
		return new TedScheduler(tedDriver);
	}

	private void initBeans(ApplicationContext ctx) {
		TedDriver tedDriver = ctx.getBean(TedDriver.class);
		TedScheduler tedScheduler = ctx.getBean(TedScheduler.class);

		TedTaskManager tedTaskManager = ctx.getBean(TedTaskManager.class);
		tedTaskManager.setTedDriver(tedDriver);

		TaskRegistrar registrar = new TaskRegistrar(ctx, tedDriver, tedScheduler);
		registrar.registerAnnotatedTasks();

		// start tedDriver (but still tasks will start with few sec delay)
		logger.debug("Starting TedDriver");
		tedDriver.start();
	}


	@Override
	public void setApplicationContext(ApplicationContext ctx) throws BeansException {
		initBeans(ctx);
	}


	@Override
	public void setImportMetadata(AnnotationMetadata importMetadata) {
		this.annotationAttributes = AnnotationAttributes.fromMap(importMetadata.getAnnotationAttributes(EnableTedTask.class.getName(), false));
		if (this.annotationAttributes == null) {
			throw new IllegalArgumentException("@EnableTedTask is not present on importing class " + importMetadata.getClassName());
		}
	}


	private static boolean isEmpty(final CharSequence cs) {
		return cs == null || cs.length() == 0;
	}

	private static Map<String, String> getAllKnownProperties(Environment env, String prefix) {
		Map<String, String> rtn = new HashMap<>();
		if (!(env instanceof ConfigurableEnvironment))
			return rtn;
		for (PropertySource<?> propertySource : ((ConfigurableEnvironment) env).getPropertySources()) {
			if (! (propertySource instanceof EnumerablePropertySource))
				continue;
			for (String key : ((EnumerablePropertySource) propertySource).getPropertyNames()) {
				if (! key.startsWith(prefix))
					continue;
				Object value = propertySource.getProperty(key);
				if (value != null)
					rtn.put(key, value.toString());
			}
		}
		return rtn;
	}


}
