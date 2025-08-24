FROM ubuntu:latest

# build using command: docker build --progress=plain -t chronon-base .

# Install necessary tools and Python
RUN apt update && apt install -y wget curl bash python3 python3-pip openjdk-17-jdk python3.12-venv

# java
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH=$PATH:$JAVA_HOME/bin

# thrift
ARG THRIFT_VERSION=0.21.0
RUN apt install -y \
    build-essential \
    cmake \
    libboost-dev \
    libssl-dev \
    libevent-dev \
    bison \
    flex \
    autoconf \
    automake \
    libtool \
    curl && \
    curl -LSs https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz -o thrift-${THRIFT_VERSION}.tar.gz && \
    tar -xzf thrift-${THRIFT_VERSION}.tar.gz && \
    cd thrift-${THRIFT_VERSION} && \
    ./configure --without-python --without-cpp --without-nodejs --without-java && \
    make && \
    make install && \
    cd .. && \
    rm -rf thrift-${THRIFT_VERSION} thrift-${THRIFT_VERSION}.tar.gz && \
    apt purge -y \
    build-essential \
    cmake \
    libboost-dev \
    libssl-dev \
    libevent-dev \
    bison \
    flex \
    autoconf \
    automake \
    libtool && \
    apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*

RUN apt update && apt install -y build-essential git
RUN mkdir -p /usr/lib/jvm && ln -s /usr/lib/jvm/java-17-openjdk-amd64/ /usr/lib/jvm/default-jvm

# Verify installations
RUN java -version && \
    thrift -version && \
    python3 --version  && \
    pip3 --version && \
    git --version

# set system wide encoding to UTF-8
# needed by javac / jar assembly to handle files/classes with non-ascii chars
# shapeless is the main culprit
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# Set working directory
WORKDIR /app

# Cmd to run when starting the container
CMD ["/bin/bash"]
