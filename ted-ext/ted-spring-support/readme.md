# Ted spring support

## About

This module helps easily integrate ted-driver into Spring.

#### Annotations
`@EnableTedTask` - annotation on @Configuration class, enable Ted driver 

`@TedTaskProcessor` - mark method as task processor

`@TedSchedulerProcessor` - mark method as scheduler processor

`@TedDataSource` - mark dataSource for TED (in few dataSources in project case)

#### Beans
`TedTaskFactory` - task creation helper

#### Exceptions
`TedRetryException` - default retry exception (will send task to retry)

## Example
(see ted-sample4)

1. Add dependency to `ted-spring-support`

```xml
<dependency>
   <groupId>com.github.labai</groupId>
   <artifactId>ted-spring-support</artifactId>
   <version>0.3.2</version>
</dependency>
```

2. Add annotation `@EnableTedTask` 
 
```java
@EnableTedTask
@Configuration
public class TedConfig {
   
}
```

3. Register tasks using `@TedTaskProcessor` with _name_ of task.


```java
public class Tasks {
    
	@TedTaskProcessor(name = "TASK1")
	public TedResult task1(TedTask task) {
		logger.info("start TASK1: {}", task.getData());
		return TedResult.done();
	}

	@TedTaskProcessor(name = "TASK2", retryException = { IllegalStateException.class })
	public String task2(TedTask task) {
		logger.info("start TASK2");
		throw new IllegalStateException("should retry");
	}
}
```

4. Register scheduler tasks using `@TedSchedulerProcessor`.
Mandatory parameters - _name_ (scheduler task name) and _cron_ 
(cron retry expression)
  
```java
public class SchedulerTasks {
    
	@TedSchedulerProcessor(name = "SCH_1", cron = "1 * * * * *")
	public String schedulerTask1() {
		logger.info("Start schedulerTask1");
		return "ok";
	}

}
```

5. Get bean `TedTaskFactory` and create tasks.
For more complex cases a `TedTaskBuilder` can be used.

```java
@Service
public class SomeService {

	@Autowired
	private TedTaskFactory tedTaskFactory;

	private void createTasks() {
        tedTaskFactory.createTask("TASK1", gson.toJson(param));

        tedTaskFactory.taskBuilder("TASK4")
                .data(gson.toJson(param))
                .key1("key1")
                .create();
	}

}
```
