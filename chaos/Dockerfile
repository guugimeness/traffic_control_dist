FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y iproute2 iputils-ping bash && \
    apt-get clean

COPY chaos.sh /chaos.sh

RUN chmod +x /chaos.sh

CMD ["/chaos.sh"]
