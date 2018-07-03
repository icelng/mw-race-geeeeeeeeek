#!/bin/bash

set -x

java -jar \
     -Xms2G \
     -Xmx2G \
     -Ddata.url=http://nginx/trace.dat \
     -Doutput.dir=/root/result \
     /root/dists/processor.jar
