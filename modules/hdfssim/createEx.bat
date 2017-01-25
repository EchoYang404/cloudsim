@echo off
::call mvn package -Dmaven.test.skip=true -Dmaven.javadoc.skip=true
::call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="50 1"
::call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="100 1"
::call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="150 1"
::call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="200 1"
::call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="250 1"
::call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="300 1"

call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="200 1"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="400 1"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="600 1"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="800 1"
call mvn exec:java -Dexec.mainClass="org.bjut.hdfssim.experiment.CreateExConfig" -Dexec.args="1000 1"