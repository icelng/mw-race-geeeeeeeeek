#!/bin/bash

set -x

java -jar \
     -Xms512M \
     -Xmx512M \
     -Ddata.url=http://nginx/trace.dat \
     -Doutput.dir=/root/result \
     /root/dists/processor.jar
