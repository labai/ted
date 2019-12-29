# Additional features

In addition to main TED functionality - tasks execution - there are some additional useful functionality, build in TED.
But it works with _PostgreSQL_ db only.

Usually these features use _tedtask_ table but with specific channels.

### Events queue
With PostgreSQL db only!

There are functionality for tasks, which must be executed in exactly same sequence, as they were created.
In TED these tasks called _events_ and created using `createEvent` api method.
Events have _queueId_ - some id of object, for which queue will be formed, i.e. there can be many queues - each for queueId. 
When few events created for one queueId, the first of them will be executed, while others will for successful finish of it.
After finish of first event, next will be processed.
For queueId _tedtask_ column _key1_ is used and special unique index `ix_ted_queue_uniq` is created for it.

For event queues _TedEQ_ channel is used. It can be configured in properties as other channels. 

NB! If event finishes with RETRY or ERROR, then all queue by this queueId _**will be blocked**_ - next events will not be executed, unless the first event finish successfully. 
While RETRY will retry later, ERROR will stop processing this queue until manual fix.


### Prime instance
With PostgreSQL db only!

_Prime instance_ feature allows to have one prime ("master") instance between few instances.
It can be used in such ways:
* channels can have flag "prime=yes", what means these tasks will be executed only on prime instance;
* app can have own logic, but can use TED to check, is this instance prime (method `isPrime()`).

It can be useful in some situations:
* help to avoid locking, race-condition problems for some tasks, like data maintenance;
* cases, when you can have only one connection to external resource;
* can precise tune workers count (workers count will not grow with each new instance);

When prime is enabled (use `enablePrime`), TED regularly checks in db which instance is prime.
If there are no prime, then any instance will try to become prime.
After instance became prime, the event `onBecomePrime` will be called.

Switching to other instance will happen in few seconds.


### Instances notification
With PostgreSQL db only!

_Instances notification_ allows to send notification messages to all active instances.
Example of usage - notify all instances to clear some cache.

Notification message will be processed in all active instances and will disappear in few seconds, i.e. if instance will be started later, it will not be executed. 
Method `sendNotification`.
