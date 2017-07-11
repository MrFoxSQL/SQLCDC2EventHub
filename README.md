# SQL CDC 2 Event Hub

Send SQL Change Data Capture (CDC) system tracked data changes into an Azure Event Hub for downstream processing

Please see associated blog post for more information on this application
https://mrfoxsql.wordpress.com/TBC

Instructions;
(1) Download the solution files
(2) Open the solution in Visual Studio (2015)
(3) Edit the "app.config" file...
    - ExecutionControl - 1 = run continuously, 0 = run once then exit
    - ExecutionControlSleepMs - milliseconds sleep between each iteration when program is set to run continuously
    - DataTableName - the name of the source SQL Server table in "owner.table" format
    - SQLBatchSize - the number of SQL CDC change rows to bundle into a single JSON Event Hub message
    - sqlDatabaseConnectionString - the connection string for the source SQL Server
    - Microsoft.ServiceBus.EventHubToUse - the name of the target Azure Event Hub
    - Microsoft.ServiceBus.ServiceBusConnectionString - the connection string for the target Azure Event Hub
(4) Compile the solution (release)
(5) The "SQL2AEH.exe" program will be in the "...\release\bin" folder
(6) Schedule the "SQL2AEH.exe" in a SQL Agent Job on the source SQL Server machine

NOTE TO CODE: I am not a seasoned c# developer.  As such the application code is provided free without any support or warranty of any kind.   The code has not been thoroughly tested and is not considered production ready.   The code is provided free of charge and can be reused in any way you wish.   Please check my the disclaimer in my blog post above to learn more.
