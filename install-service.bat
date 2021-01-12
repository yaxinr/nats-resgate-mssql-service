sc.exe create nats-resgate-mssql binPath= "%~dp0target\debug\nats-resgate-mssql-service.exe"
sc.exe start nats-resgate-mssql
pause