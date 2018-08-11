# TedDriver API functions

Constructor
TedDriver(TedDbType dbType, DataSource dataSource, Properties properties)
* dbType - POSTGRES or ORACLE
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
* createBatch - create few tasks and additional batch task, which will be executed after all regular tasks will be finished
* newTedTask - helper to create TedTask for createBatch

With PostgreSQL db only
* createEvent - create event for queue `queueId` 
* createAndTryExecuteEvent - create event and try execute it in same thread, if possible

_Prime instance_ handling (with PostgreSQL db only):
* enablePrime - enable _prime instance_ handling
* setOnBecomePrimeHandler - set event handle for becoming prime instance
* setOnLostPrimeHandler - set event handler for losing prime instance
* isPrime - returns flag, is current app instance prime
