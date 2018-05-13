#! /bin/bash

sudo -u hduser /opt/fts/hadoop/sbin/hadoop-daemon.sh start namenode;
sudo -u hduser /opt/fts/hadoop/sbin/hadoop-daemon.sh start datanode;
sudo -u hduser /opt/fts/hadoop/sbin/hadoop-daemon.sh start secondarynamenode;
sudo -u hduser /opt/fts/hadoop/sbin/yarn-daemon.sh start resourcemanager;
sudo -u hduser /opt/fts/hadoop/sbin/yarn-daemon.sh start nodemanager;
sudo -u hduser /opt/fts/hadoop/sbin/mr-jobhistory-daemon.sh start historyserver
exit $?
