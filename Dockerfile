FROM alpine:3.15.0

RUN apk add --no-cache netcat-openbsd curl net-tools
RUN apk add --no-cache tzdata

ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN mkdir -p /logs
RUN mkdir -p /conf
COPY ./thingspanel-TDengine /
COPY ./conf/config.yaml /conf
EXPOSE 50052
CMD [ "./thingspanel-TDengine" ]