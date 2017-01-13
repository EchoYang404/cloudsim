@echo off
call mvn package -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.Default"
pause