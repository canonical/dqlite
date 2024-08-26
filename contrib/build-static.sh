#!/bin/bash -xeu

DIR="${DIR:=$(realpath `dirname "${0}"`)}"

REPO_MUSL="https://git.launchpad.net/musl"
REPO_LIBTIRPC="https://salsa.debian.org/debian/libtirpc.git"
REPO_LIBNSL="https://github.com/thkukuk/libnsl.git"
REPO_LIBUV="https://github.com/libuv/libuv.git"
REPO_LIBLZ4="https://github.com/lz4/lz4.git"
REPO_SQLITE="https://github.com/sqlite/sqlite.git"
REPO_DQLITE="https://github.com/cole-miller/dqlite.git"

TAG_MUSL="v1.2.4"
TAG_LIBTIRPC="upstream/1.3.3"
TAG_LIBNSL="v2.0.1"
TAG_LIBUV="v1.48.0"
TAG_LIBLZ4="v1.9.4"
TAG_SQLITE="version-3.45.1"
TAG_DQLITE="musl-ci"

BUILD_DIR="${DIR}/build"
INSTALL_DIR="${DIR}/prefix"
mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" "${INSTALL_DIR}/lib" "${INSTALL_DIR}/include"
BUILD_DIR="$(realpath "${BUILD_DIR}")"
INSTALL_DIR="$(realpath "${INSTALL_DIR}")"

export CFLAGS=""
MACHINE_TYPE="$(uname -m)"
if [ "${MACHINE_TYPE}" = "ppc64le" ]; then
  MACHINE_TYPE="powerpc64le"
  export CFLAGS="-mlong-double-64"
fi
export PKG_CONFIG_PATH="${INSTALL_DIR}/lib/pkgconfig"

# build musl
if [ ! -f "${INSTALL_DIR}/musl/bin/musl-gcc" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf musl
    git clone "${REPO_MUSL}" --depth 1 --branch "${TAG_MUSL}" musl
    cd musl
    ./configure --prefix="${INSTALL_DIR}/musl"
    make -j
    make -j install

    # missing musl header files
    ln -s "/usr/include/${MACHINE_TYPE}-linux-gnu/sys/queue.h" "${INSTALL_DIR}/musl/include/sys/queue.h" || true
    ln -s "/usr/include/${MACHINE_TYPE}-linux-gnu/asm" "${INSTALL_DIR}/musl/include/asm" || true
    ln -s /usr/include/asm-generic "${INSTALL_DIR}/musl/include/asm-generic" || true
    ln -s /usr/include/linux "${INSTALL_DIR}/musl/include/linux" || true
  )
fi

export PATH="${PATH}:${INSTALL_DIR}/musl/bin"
export CFLAGS="${CFLAGS} -isystem ${INSTALL_DIR}/musl/include"
export CC=musl-gcc
export LDFLAGS=-static

# build libtirpc
if [ ! -f "${BUILD_DIR}/libtirpc/src/libtirpc.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libtirpc
    git clone "${REPO_LIBTIRPC}" --depth 1 --branch "${TAG_LIBTIRPC}"
    cd libtirpc
    chmod +x autogen.sh
    ./autogen.sh
    ./configure --disable-shared --disable-gssapi --prefix="${INSTALL_DIR}"
    make -j install
  )
fi

# build libnsl
if [ ! -f "${BUILD_DIR}/libnsl/src/libnsl.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libnsl
    git clone "${REPO_LIBNSL}" --depth 1 --branch "${TAG_LIBNSL}"
    cd libnsl
    ./autogen.sh
    autoreconf -i
    autoconf
    ./configure --disable-shared --prefix="${INSTALL_DIR}"
    make -j install
  )
fi

# build libuv
if [ ! -f "${BUILD_DIR}/libuv/libuv.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libuv
    git clone "${REPO_LIBUV}" --depth 1 --branch "${TAG_LIBUV}"
    cd libuv
    ./autogen.sh
    ./configure --disable-shared --prefix="${INSTALL_DIR}"
    make -j install
  )
fi

# build liblz4
if [ ! -f "${BUILD_DIR}/lz4/lib/liblz4.a" ] || [ ! -f "${BUILD_DIR}/lz4/lib/liblz4.so" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf lz4
    git clone "${REPO_LIBLZ4}" --depth 1 --branch "${TAG_LIBLZ4}"
    cd lz4
    make install -j PREFIX="${INSTALL_DIR}" BUILD_SHARED=no
  )
fi

# build sqlite3
if [ ! -f "${BUILD_DIR}/sqlite/libsqlite3.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf sqlite
    git clone "${REPO_SQLITE}" --depth 1 --branch "${TAG_SQLITE}"
    cd sqlite
    ./configure --disable-shared --disable-readline --prefix="${INSTALL_DIR}" \
      CFLAGS="${CFLAGS} -DSQLITE_ENABLE_DBSTAT_VTAB=1"
    make install -j BCC="${CC} -g -O2 ${CFLAGS} ${LDFLAGS}"
  )
fi

# build dqlite
if [ ! -f "${BUILD_DIR}/dqlite/libdqlite.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf dqlite
    git clone "${REPO_DQLITE}" --depth 1 --branch "${TAG_DQLITE}"
    cd dqlite
    autoreconf -i
    ./configure --disable-shared --enable-build-raft --prefix="${INSTALL_DIR}"

    # Don't run the raft addrinfo tests since they rely on libc being
    # dynamically linked.
    bins="unit-test integration-test \
          raft-core-fuzzy-test \
          raft-core-integration-test \
          raft-core-unit-test \
          raft-uv-integration-test \
          raft-uv-unit-test"
    make -j LDFLAGS=-all-static $bins
    for bin in $bins
    do LIBDQLITE_TRACE=1 ./$bin || touch any-failed
    done
    test '!' -e any-failed
  )
fi
