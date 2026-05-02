@echo off
echo Starting Zookeeper...
start "Zookeeper" cmd /c "%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties"
timeout /t 5 /nobreak

echo Starting Kafka...
start "Kafka" cmd /c "%KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties"
timeout /t 5 /nobreak

echo Creating topic game-ratings...
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic game-ratings --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>nul || echo Topic may already exist.

echo Kafka ready.
