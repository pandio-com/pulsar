FROM openjdk:8-jdk-slim as pulsar

ARG PULSAR_IO_DIR=apache-pulsar-io-connectors-2.5.2-bin
ARG PULSAR_OFFLOADER_TARBALL=apache-pulsar-offloaders-2.5.2-bin.tar.gz
ARG PULSAR_TARBALL=apache-pulsar-2.5.2-bin.tar.gz

ADD ${PULSAR_TARBALL} /
ADD ${PULSAR_IO_DIR} /pulsar/connectors
ADD ${PULSAR_OFFLOADER_TARBALL} /
RUN mv /apache-pulsar-offloaders-*/offloaders /pulsar/offloaders

COPY scripts/apply-config-from-env.py /pulsar/bin
COPY scripts/gen-yml-from-env.py /pulsar/bin
COPY scripts/generate-zookeeper-config.sh /pulsar/bin
COPY scripts/pulsar-zookeeper-ruok.sh /pulsar/bin
COPY scripts/watch-znode.py /pulsar/bin
COPY scripts/set_python_version.sh /pulsar/bin
# COPY scripts/install-pulsar-client-27.sh /pulsar/bin
# COPY scripts/install-pulsar-client-37.sh /pulsar/bin

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