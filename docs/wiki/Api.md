# TedDriver API functions

### Simple api

Constructor
TedDriver(DataSource dataSource, Properties properties)
* dataSource - datasource to db with tedtask table
* properties - TedDriver configuration (e.g., can be read from ted.properties file)

Initialization
* registerTaskConfig - register task - assign processor
* start - start Ted (once after app startup)
* shutdown - on app shutdown (can be called by spring)

Runtime
* createTask - create task to be executed
* createTaskPostponed - create future task
* createAndExecuteTask - create and execute task immediately in the same thread - will wait synchronous for finish
* createAndStartTask - create task and add to execution queue - will not wait for finish

Builders, helpers
TedTaskHelper taskHelper = new TedTaskHelper(tedDriver); 
E.g. taskHelper.getTaskFactory().taskBuilder(TASK_NAME).create();

### Spring api

#### Annotations

@EnableTedTask - annotation on @Configuration class

@TedTaskProcessor - mark method as task processor

@TedSchedulerProcessor - mark method as scheduler processor

@TedDataSource - mark dataSource for TED (in few dataSources in project case)

#### Beans
TedTaskFactory - task creation helper

#### Exceptions
TedRetryException - default retry exception (will send task to retry)
