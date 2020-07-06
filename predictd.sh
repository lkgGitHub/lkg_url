#!/bin/bash
proc_name='com.lkg.Predict'

proc_num=`ps -ef|grep $proc_name|grep -v grep|wc -l`
if [ $proc_num -eq 0 ]
then
    cd /data/prj_sca/hj/ && sh predict.sh
    echo `date +%Y-%m-%d` `date +%H:%M:%S`  "restart $proc_name" >> monitor.log
fi