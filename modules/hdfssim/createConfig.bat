@echo off
call mvn package -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
::call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateRackConfig" -Dexec.args="64 3"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateRackConfig" -Dexec.args="128 3"
pause