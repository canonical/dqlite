# FROM debian:buster-slim as dqlite-lib-builder 
FROM ubuntu as dqlite-lib-builder 
ARG DEBIAN_FRONTEND="noninteractive"
ENV TZ=Europe/London
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV GOROOT=/usr/local/go
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$GOROOT/bin:$PATH

RUN apt-get update && apt-get install -y git build-essential dh-autoreconf pkg-config libuv1-dev tcl8.6 wget

WORKDIR /opt

RUN git clone --depth 100 https://github.com/canonical/sqlite.git

WORKDIR /opt/sqlite

RUN ./configure --enable-replication && make && make install

WORKDIR /opt

RUN git clone https://github.com/canonical/libco.git

WORKDIR /opt/libco

RUN make && make install

WORKDIR /opt

RUN git clone https://github.com/canonical/raft.git

WORKDIR /opt/raft

RUN autoreconf -i && ./configure && make && make install

WORKDIR /opt

RUN git clone https://github.com/canonical/dqlite.git

WORKDIR /opt/dqlite

RUN autoreconf -i && ./configure && make && make install

WORKDIR /opt

RUN git clone https://github.com/canonical/go-dqlite.git

WORKDIR /opt

RUN wget -c https://golang.org/dl/go1.15.2.linux-amd64.tar.gz -O - | tar -xzf - -C /usr/local

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
COPY --from=dqlite-lib-builder /usr/local/lib /usr/local/lib
COPY --from=dqlite-lib-builder \
    /usr/lib/libco.so.0.1.0 \
    /usr/lib/pkgconfig/libco.pc \
    /usr/lib/libco.so.0 \
    /usr/lib/libco.so \
    /usr/lib/libco.a \
    /usr/lib/x86_64-linux-gnu/libuv.so \
    /usr/lib/x86_64-linux-gnu/libuv.so.1\
    /usr/lib/x86_64-linux-gnu/libuv.so.1.0.0\
    /usr/lib/

COPY --from=dqlite-lib-builder \    
    /usr/include/libco.h \
    /usr/include/    