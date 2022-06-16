# FROM debian:buster-slim as dqlite-lib-builder 
FROM ubuntu as dqlite-lib-builder 
ARG DEBIAN_FRONTEND="noninteractive"
ENV TZ=Europe/London
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV GOROOT=/usr/local/go
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$GOROOT/bin:$PATH

RUN apt-get update && apt-get install -y git build-essential dh-autoreconf pkg-config libuv1-dev libsqlite3-dev liblz4-dev tcl8.6 wget

WORKDIR /opt

RUN git clone https://github.com/canonical/raft.git && \
    git clone https://github.com/canonical/go-dqlite.git && \
    wget -c https://golang.org/dl/go1.15.2.linux-amd64.tar.gz -O - | tar -xzf - -C /usr/local

WORKDIR /opt/raft

RUN autoreconf -i && ./configure && make && make install

WORKDIR /opt/dqlite

COPY . .

RUN autoreconf -i && ./configure && make && make install

WORKDIR /opt/go-dqlite

RUN go get -d -v ./... && \
    go install -tags libsqlite3 ./cmd/dqlite-demo && \
    go install -tags libsqlite3 ./cmd/dqlite

# FROM debian:buster-slim 
FROM ubuntu
ARG DEBIAN_FRONTEND="noninteractive"
ENV TZ=Europe/London
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PATH=/opt:$PATH

COPY --from=dqlite-lib-builder /go/bin /opt/
COPY --from=dqlite-lib-builder /usr/local/lib /usr/local/lib
COPY --from=dqlite-lib-builder \
    /usr/lib/x86_64-linux-gnu/libuv.so \
    /usr/lib/x86_64-linux-gnu/libuv.so.1\
    /usr/lib/x86_64-linux-gnu/libuv.so.1.0.0\
    /usr/lib/

COPY --from=dqlite-lib-builder \
    /lib/x86_64-linux-gnu/libsqlite3.so \
    /lib/x86_64-linux-gnu/libsqlite3.so.0 \
    /usr/lib/x86_64-linux-gnu/  
