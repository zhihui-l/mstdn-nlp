FROM apache/spark-py:v3.4.0
USER root
RUN pip install fastapi uvicorn[standard]
WORKDIR /opt/app
COPY server.sh .
RUN chmod a+rx server.sh
ADD mnapi/ mnapi/
ENTRYPOINT [ "/bin/sh", "-c"]
CMD ["./server.sh"]

