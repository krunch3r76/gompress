# Dockerfile for spyu

FROM alpine:latest
VOLUME /golem/workdir /golem/output
COPY xz.sh /root
RUN chmod +x /root/xz.sh
RUN apk add --no-cache bash xz
WORKDIR /golem/workdir
# RUN apk add --no-cache jq
