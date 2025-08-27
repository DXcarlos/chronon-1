FROM ubuntu:latest

# build using command: docker build --progress=plain -t chronon-base .

# Install necessary tools and Python
RUN apt update && apt install -y wget curl bash python3 python3-pip openjdk-11-jdk python3.12-venv

# java
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH=$PATH:$JAVA_HOME/bin

# TODO(nikhil) remove bazel
RUN curl -fsSL "https://github.com/bazelbuild/bazelisk/releases/download/v1.18.0/bazelisk-linux-amd64" -o /usr/local/bin/bazel
RUN chmod +x /usr/local/bin/bazel
ENV PATH="/usr/local/bin:${PATH}"

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
    libtool \
    curl && \
    apt autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Install uv - fast Python package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

# Upgrade pip and install some common Python packages
RUN pip3 install --break-system-packages pytest tox flake8 ruff

RUN apt update && apt install -y build-essential git
RUN mkdir -p /usr/lib/jvm && ln -s /usr/lib/jvm/java-11-openjdk-amd64/ /usr/lib/jvm/default-jvm

# Verify installations
RUN java -version && \
    thrift -version && \
    python3 --version  && \
    pip3 --version && \
    git --version

# Set working directory
WORKDIR /app

# Cmd to run when starting the container
CMD ["/bin/bash"]
