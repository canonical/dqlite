#!/bin/bash -xeu

DIR="${DIR:=$(realpath `dirname "${0}"`)}"

REPO_MUSL="https://git.launchpad.net/musl"
REPO_LIBTIRPC="https://salsa.debian.org/debian/libtirpc.git"
REPO_LIBNSL="https://github.com/thkukuk/libnsl.git"
REPO_LIBUV="https://github.com/libuv/libuv.git"
REPO_LIBLZ4="https://github.com/lz4/lz4.git"
REPO_SQLITE="https://github.com/sqlite/sqlite.git"

TAG_MUSL="${TAG_MUSL:-v1.2.4}"
DQLITE_PATH="${DQLITE_PATH:-$DIR/..}"

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

clone-latest-tag() {
	name="$1"
	repo="$2"
	tagpattern="${3:-.*}"
	mkdir "${name}"
	pushd "${name}"
	git init
	git remote add upstream "${repo}"
	git fetch upstream 'refs/tags/*:refs/tags/*'
	tag="$(git tag | grep "${tagpattern}" | sort -V -r | head -n1)"
	echo "Selected $name tag ${tag}"
	git checkout "${tag}"
	popd
}

# build musl
if [ ! -f "${INSTALL_DIR}/musl/bin/musl-gcc" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf musl
    git clone "${REPO_MUSL}" --depth 1 --branch "${TAG_MUSL}" musl
    cd musl
    # workaround for missing stdatomic.h see https://musl.openwall.narkive.com/3RCAs95G/patch-add-stdatomic-h-for-clang-3-1-and-gcc-4-1
    cp /usr/lib/clang/18/include/stdatomic.h include/stdatomic.h
    CC=clang ./configure --prefix="${INSTALL_DIR}/musl"
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
export CC=musl-clang
export LDFLAGS=-static

# build libtirpc
if [ ! -f "${BUILD_DIR}/libtirpc/src/libtirpc.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf libtirpc
    clone-latest-tag libtirpc "${REPO_LIBTIRPC}" upstream
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
    clone-latest-tag libnsl "${REPO_LIBNSL}"
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
    clone-latest-tag libuv "${REPO_LIBUV}"
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
    clone-latest-tag lz4 "${REPO_LIBLZ4}"
    cd lz4
    make install -j PREFIX="${INSTALL_DIR}" BUILD_SHARED=no
  )
fi

# build sqlite3
if [ ! -f "${BUILD_DIR}/sqlite/libsqlite3.la" ]; then
  (
    cd "${BUILD_DIR}"
    rm -rf sqlite
    clone-latest-tag sqlite "${REPO_SQLITE}"
    cd sqlite
    ./configure --disable-shared --disable-readline --prefix="${INSTALL_DIR}" \
      CFLAGS="${CFLAGS} -DSQLITE_ENABLE_DBSTAT_VTAB=1"
    make install -j BCC="${CC} -g -O2 ${CFLAGS} ${LDFLAGS}"
  )
fi

# build dqlite
if [ ! -f "${BUILD_DIR}/dqlite/libdqlite.la" ]; then
  (
    cd "${DQLITE_PATH}"
    autoreconf -i
    ./configure --enable-build-raft --with-static-deps --prefix="${INSTALL_DIR}"
    make -j check-norun
    make check
  )
fi
