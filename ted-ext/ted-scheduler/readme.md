# Ted scheduler

## About

Helper for creating scheduler tasks.
 
Use ted-driver for task executing.
Provides simplified API for scheduler tasks,
some additional functionality, e.g. cron expressions.


```java
TedScheduler scheduler = new TedScheduler(tedDriver);
scheduler.builder().name("MAINT")
        .scheduleCron("	0 0/10 * 1/1 * ? *")
        .runnable(() -> {
            logger.info("do something");
        }).register();
```
		