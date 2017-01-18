@echo off
::call mvn package -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="50 10"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="100 10"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="150 10"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="200 10"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="250 10"