Simple sample how to use TED.

This sample process data file - will read that file, create task for each line,
and then TED will execute these tasks.

Before start it is required to prepare tedtask table:
- on PostgreSql create tedtask table - execute (ted)/docs/init/ted_db_struct_pgre.sql
- configure connection parameters in Sample1.java

TED tasks are configured in (ted-sample1)/resources/ted.properties


