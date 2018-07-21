#!/bin/bash

set -x

java -jar \
     -Xmx512M \
     -Ddata.url=http://nginx/trace.dat \
     -Doutput.dir=/root/result \
     -Dlogs.dir=/root/logs \
     -XX:+PrintGCDetails \
     /root/dists/processor.jar
