FROM openjdk:8-jdk-slim as pulsar

ENV PULSAR_VERSION=2.6.0
ENV PULSAR_IO_DIR=apache-pulsar-io-connectors-${PULSAR_VERSION}-bin
ENV PULSAR_OFFLOADER_TARBALL=apache-pulsar-offloaders-${PULSAR_VERSION}-bin.tar.gz
ENV PULSAR_TARBALL=apache-pulsar-${PULSAR_VERSION}-bin.tar.gz

ADD ${PULSAR_TARBALL} /
ADD ${PULSAR_IO_DIR} /pulsar/connectors/
ADD ${PULSAR_OFFLOADER_TARBALL} /
RUN mv /apache-pulsar-offloaders-*/offloaders /pulsar/offloaders

COPY scripts/* /pulsar/bin/

# Install some utilities
RUN apt-get update \
     && apt-get install -y netcat dnsutils less procps iputils-ping \
               #   python2.7 python-setuptools python-yaml python-kazoo \
                 python3.7 python3-setuptools python3-yaml python3-kazoo \
                 libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev \
                 curl \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
# RUN python2.7 get-pip.py
RUN python3.7 get-pip.py

# ADD target/python-client/ /pulsar/pulsar-client
# ADD target/cpp-client/ /pulsar/cpp-client
# RUN /pulsar/bin/install-pulsar-client-27.sh
# RUN /pulsar/bin/install-pulsar-client-37.sh
RUN echo networkaddress.cache.ttl=1 >> $JAVA_HOME/jre/lib/security/java.security
# RUN apt-get update \
#      && apt install -y /pulsar/cpp-client/*.deb \
#      && apt-get clean \
#      && rm -rf /var/lib/apt/lists/*

WORKDIR /pulsar

VOLUME  ["/pulsar/conf", "/pulsar/data"]

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE
