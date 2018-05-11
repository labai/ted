drop table tedtask;
drop sequence seq_tedtask_id;
drop sequence seq_tedtask_bno;
/

create sequence seq_tedtask_id minvalue 1000 maxvalue 999999999999 start with 1001 increment by 1;
create sequence seq_tedtask_bno minvalue 1000 maxvalue 999999999999 start with 1001 increment by 1;
/

create table tedtask(
    taskid      number(12,0) not null,
    system      varchar2(8) not null, -- system id. e.g. dipsis, or dips.aug for dev
    name        varchar2(15) not null,
    status      varchar2(5) not null, -- NEW, WORK, RETRY, ERROR, DONE
    channel     varchar2(5), -- channel: MAIN
    --tasktp      varchar2(1) null, -- null - simple task, S - schedule, B - batch job, L - lock
    nextts      timestamp(3) default systimestamp,
    batchid     number(12,0) null,
    --parentid    number(12,0) null,
    retries     number(9,0) default 0 not null,
    key1        varchar2(30),
    key2        varchar2(30),
    createts    timestamp(3) default systimestamp not null,
    startts     timestamp(3),
    finishts    timestamp(3),
    msg         varchar2(300 char),
    data        varchar2(500 char),
    bno         number(12,0)
  )
/


-- Indexes for tedtask.
-- Warning: It is expected that tedtask table is for single app (system)
create unique index ix_tedtask_pk on tedtask (taskid);
create index ix_tedtask_batchid on tedtask (batchid, status);
--create index ix_tedtask_parentid on tedtask (parentid);
create index ix_tedtask_name on tedtask (name, status);
create index ix_tedtask_quickchk on tedtask (nextts); -- channel?
create index ix_tedtask_nextts on tedtask (channel, system, nextts);
create index ix_tedtask_key1 on tedtask (key1);
create index ix_tedtask_key2 on tedtask (key2);
create index ix_tedtask_createts on tedtask (createts);
create index ix_tedtask_bno on tedtask (bno);
--create index ix_tedtask_tasktp on tedtask (tasktp);
create index ix_tedtask_status on tedtask (status); -- maintenance task
/


-- Comments for tedtask
comment on table tedtask is 'TED (Task Execution Driver) tasks';
/
comment on column tedtask.taskid     is 'Id - primary key';
comment on column tedtask.batchid    is 'Reference to batch taskId';
--comment on column tedtask.parentid   is 'Reference to parent taskId (can make task chain)';
comment on column tedtask.system     is 'System id. E.g. dipsis, or dips.aug for dev';
comment on column tedtask.name       is 'Task name';
comment on column tedtask.status     is 'Status';
comment on column tedtask.msg        is 'Status message';
comment on column tedtask.createts   is 'Record create timestamp';
comment on column tedtask.startts    is 'Execution start timestamp';
comment on column tedtask.finishts   is 'Execution finish timestamp';
comment on column tedtask.retries    is 'Retry count';
comment on column tedtask.nextts     is 'Postponed execute (next retry) timestamp';
comment on column tedtask.key1       is 'Search key 1 (task specific)';
comment on column tedtask.key2       is 'Search key 2 (task specific)';
comment on column tedtask.data       is 'Task data (parameters)';
comment on column tedtask.bno        is 'Batch number (system field)';
--comment on column tedtask.tasktp     is 'Record type (null - simple task, S - schedule, B - batch job, L - lock)';
/

-- insert into tedtask(taskid, system, name, status, channel, tasktp, nextts, batchid, retries, key1, key2, msg, createts, startts, finishts, data, bno)
-- values (1, 'ted:sys', 'ted:lock', 'DONE', null, 'L', null, null, 0, null, null, 'TED system record', systimestamp, null, null, null, 0);
-- commit;
-- /
