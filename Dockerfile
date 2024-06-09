FROM alpine:3.9
RUN mkdir -p /logs
RUN mkdir -p /conf
COPY ./thingspanel-TDengine /
COPY ./conf/config.yaml /conf
EXPOSE 50052
CMD [ "./thingspanel-TDengine" ]