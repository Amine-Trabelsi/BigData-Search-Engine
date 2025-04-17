FROM firasj/spark-docker-cluster

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install cassandra-driver pyspark && \
    ln -s /usr/bin/python3 /usr/bin/python
