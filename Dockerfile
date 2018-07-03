# Builder container
FROM registry.cn-hangzhou.aliyuncs.com/aliware2018/debian-jdk8-devel AS builder

COPY . /root/workspace
WORKDIR /root/workspace
RUN set -ex && mvn clean package


# Runner container
FROM registry.cn-hangzhou.aliyuncs.com/aliware2018/debian-jdk8

COPY --from=builder /root/workspace/target/mwrace2018-geeks-demo-1.0.0-SNAPSHOT.jar /root/dists/geeks.jar
COPY docker-entrypoint.sh /usr/local/bin

RUN set -ex \
 && chmod a+x /usr/local/bin/docker-entrypoint.sh \
 && mkdir -p /root/logs

ENTRYPOINT ["docker-entrypoint.sh"]
