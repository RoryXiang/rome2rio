#!/bin/sh
echo 'hello'
mm=0
while true

do
    DATA=$(date +%Y-%m-%d:%H:%M:%S)
    procnum=` ps -ef|grep "scrapy"|grep -v grep|wc -l`
#    echo $procnum
    if [ $procnum == $mm ]
    then
        echo 'spider stoped at:' $DATA
        nohup scrapy crawl rome2rio &
    fi
    sleep 30
done
