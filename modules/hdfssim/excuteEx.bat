@echo off
for /R %%s in (ex_*.json) do (
::echo %%s
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.Default" -Dexec.args=%%s
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.Select" -Dexec.args=%%s
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.DefaultWithMigrate" -Dexec.args=%%s
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.SelectWithMigrate" -Dexec.args=%%s
)
pause