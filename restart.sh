#!/bin/bash
while :       #循环，为了让脚本一直运行监控
do
  COUNT=`ps -ef | grep ./MysqlSync |wc -l`
  if [ "$COUNT" -gt 1 ];
  then
    echo "server service is ok"
  else
    echo "server servicie not exist"
    nohup ./MysqlSync > reStart.log 2>&1 &
  fi
  sleep 60
done