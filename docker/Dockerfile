ARG MANYLINUX_IMAGE_BASE_URL=quay.io/pypa/
ARG MANYLINUX_IMAGE_TAG=2024.12.17-1
ARG MANYLINUX_TAG=manylinux_2_28
FROM --platform=linux/amd64 ${MANYLINUX_IMAGE_BASE_URL}${MANYLINUX_TAG}_x86_64:${MANYLINUX_IMAGE_TAG} AS base-amd64
FROM --platform=linux/arm64 ${MANYLINUX_IMAGE_BASE_URL}${MANYLINUX_TAG}_aarch64:${MANYLINUX_IMAGE_TAG} AS base-arm64

FROM base-$TARGETARCH

ENV PIPX_DEFAULT_PYTHON=python3.12
ENV PYTHONUNBUFFERED=1
ENV CARGO_ROOT=/root/.cargo
ENV PATH=${CARGO_ROOT}/bin:$PATH

# Ensure Python versions we need are installed, and we update python3 link
RUN manylinux-interpreters ensure cp37-cp37m cp38-cp38 cp39-cp39 cp310-cp310 cp311-cp311 cp312-cp312 cp313-cp313 \
    && ln -s /usr/bin/${PIPX_DEFAULT_PYTHON} /usr/bin/python3

# Install jq
RUN curl -L https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-${TARGETARCH} -o /usr/local/bin/jq && \
    chmod +x /usr/local/bin/jq

# Install system dependencies
RUN dnf install -y epel-release \
    && curl -fsSL https://rpm.nodesource.com/setup_lts.x | sh -s \
    && dnf install -y \
      ca-certificates \
      enchant-devel \
      gdb \
      libffi-devel \
      libssh2-devel \
      mariadb-devel \
      libmemcached-devel \
      ncurses-devel \
      nodejs \
      openssl-devel \
      postgresql-devel \
      readline-devel \
      sqlite-devel \
      wget \
      xz-devel \
      zlib-devel && \
    dnf -y clean all && rm -fr /var/cache

# Allow running datadog-ci in CI with npx
RUN npm install -g @datadog/datadog-ci

# Install azure-functions-core-tools-4, only supported on amd64 architecture for Linux
# https://github.com/Azure/azure-functions-core-tools/issues/3112
RUN if [ "TARGETARCH" = "amd64" ]; \
  then \
    npm i -g azure-functions-core-tools@4 --unsafe-perm true; \
  fi

# Google Chrome is needed for selenium contrib tests but is currently only available on amd64
RUN if [ "$TARGETARCH" = "amd64" ]; \
  then \
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm \
    && dnf install -y ./google-chrome-stable_current_x86_64.rpm \
    && rm -f ./google-chrome-stable_current_x86_64.rpm; \
  fi

# Install Rust toolchain
RUN curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y

# Install Hatch
ARG HATCH_VERSION=1.14.0
ARG RIOT_VERSION=0.20.1
RUN pipx install hatch==${HATCH_VERSION} riot==${RIOT_VERSION} virtualenv

CMD ["/bin/bash"]
