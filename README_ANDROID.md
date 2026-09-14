# Building dqlite for Android

This document describes how to cross-compile dqlite for Android with the Android
NDK, Conan, and the portable dqlite backends.

This is a cross-build validation document. It does not claim that runtime tests
have passed on an Android emulator or physical device.

## Supported targets

Validated targets:

| Android ABI | Conan arch | API level | Result |
| --- | --- | ---: | --- |
| `arm64-v8a` | `armv8` | 24 | `libdqlite.la` and `.libs/libdqlite.a` build |
| `armeabi-v7a` | `armv7` | 24 | `libdqlite.la` and `.libs/libdqlite.a` build |

Validated dependency versions:

- `sqlite3/3.53.3`
- `libuv/1.51.0`

Validated backend selection:

- dqlite VFS provider: `portable`
- Raft writer backend: default portable LibUV backend

## Prerequisites

Install the equivalent tools for your platform or provide them in a container:

- Android SDK and Android NDK
- Conan 2.x
- Autotools: `autoconf`, `automake`, `libtool`, `pkg-config`
- Build tools: `make`, `tar`, `sed`, `grep`

Set `ANDROID_NDK_HOME` to your installed NDK path:

```sh
export ANDROID_NDK_HOME=/path/to/android-sdk/ndk/<version>
```

The validated NDK version was `27.2.12479018`, but the commands below are
written so contributors can use their own NDK path.

## Conan remotes

Ensure ConanCenter is enabled before installing dependencies:

```sh
conan remote enable conancenter || true
```

If your Conan installation has private remotes configured, make sure they do not
override the public ConanCenter recipes unless that is intentional for your
environment.

## Dependency recipe

Create a temporary dependency workspace outside the source tree:

```sh
export DQLITE_SRC=$PWD
export DQLITE_ANDROID_ROOT=${DQLITE_ANDROID_ROOT:-${TMPDIR:-/tmp}/dqlite-android}

rm -rf "$DQLITE_ANDROID_ROOT"
mkdir -p "$DQLITE_ANDROID_ROOT/deps"
cd "$DQLITE_ANDROID_ROOT/deps"
```

Create `conanfile.txt`:

```sh
cat > conanfile.txt <<'EOF'
[requires]
sqlite3/3.53.3
libuv/1.51.0

[generators]
PkgConfigDeps
AutotoolsDeps
VirtualBuildEnv
VirtualRunEnv

[options]
sqlite3/*:shared=False
libuv/*:shared=False
EOF
```

Conan emits `libuv-static.pc`; dqlite's `configure.ac` checks for `libuv`, so
the commands below create a local `libuv.pc` alias next to the generated `.pc`
files:

```sh
sed -e 's/^Name: libuv-static$/Name: libuv/' \
  "$deps/libuv-static.pc" > "$deps/libuv.pc"
```

Unlike the Windows alias, Android does not need the extra `-lole32` library.

## API level requirement

Android API 21 was tested and is not sufficient for the current Conan
`libuv/1.51.0` recipe in the validated environment. The libuv build uses
`getifaddrs`/`freeifaddrs`, which are not available in that NDK API level.

Use API 24 or newer for the validated path.

## Build Android arm64-v8a

The following commands build Android arm64-v8a dependencies and then build
`libdqlite.la`.

```sh
export DQLITE_SRC=${DQLITE_SRC:-$PWD}
export DQLITE_ANDROID_ROOT=${DQLITE_ANDROID_ROOT:-${TMPDIR:-/tmp}/dqlite-android}
export ANDROID_API=${ANDROID_API:-24}
export ANDROID_NDK_HOME=${ANDROID_NDK_HOME:?set ANDROID_NDK_HOME first}

deps="$DQLITE_ANDROID_ROOT/arm64-v8a/deps"
build="$DQLITE_ANDROID_ROOT/arm64-v8a/build"

rm -rf "$deps" "$build"
mkdir -p "$deps" "$build"
cp "$DQLITE_ANDROID_ROOT/deps/conanfile.txt" "$deps/conanfile.txt"

conan install "$deps" \
  --profile:build=default \
  --output-folder="$deps/out" \
  --build=missing \
  -s:h os=Android \
  -s:h os.api_level="$ANDROID_API" \
  -s:h arch=armv8 \
  -s:h compiler=clang \
  -s:h compiler.version=18 \
  -s:h compiler.libcxx=c++_static \
  -s:h compiler.cppstd=17 \
  -s:h build_type=Release \
  -c:h tools.android:ndk_path="$ANDROID_NDK_HOME"

pcdir="$deps/out"
sed -e 's/^Name: libuv-static$/Name: libuv/' \
  "$pcdir/libuv-static.pc" > "$pcdir/libuv.pc"

tar -C "$DQLITE_SRC" \
  --exclude=.git --exclude=.deps --exclude=.libs \
  --exclude='*.o' --exclude='*.lo' --exclude='*.la' \
  -cf - . | tar -C "$build" -xf -

cd "$build"
make distclean >/dev/null 2>&1 || true
if [ ! -x ./configure ]; then
  autoreconf -i
fi

toolchain="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64"
export CC="$toolchain/bin/aarch64-linux-android${ANDROID_API}-clang"
export CXX="$toolchain/bin/aarch64-linux-android${ANDROID_API}-clang++"
export AR="$toolchain/bin/llvm-ar"
export RANLIB="$toolchain/bin/llvm-ranlib"
export STRIP="$toolchain/bin/llvm-strip"
export PKG_CONFIG_PATH="$pcdir"
export PKG_CONFIG_LIBDIR="$pcdir"
export CFLAGS="--sysroot=$toolchain/sysroot"
export LDFLAGS="--sysroot=$toolchain/sysroot"

./configure \
  --host=aarch64-linux-android \
  --build=x86_64-pc-linux-gnu \
  --with-vfs-shm=portable \
  --disable-backtrace \
  --without-lz4 \
  --with-static-deps

make -j"$(nproc)" libdqlite.la
file .libs/libdqlite.a
```

Expected final artifact summary:

```text
.libs/libdqlite.a: current ar archive
```

## Build Android armeabi-v7a

The 32-bit ARM build uses Conan arch `armv7`, NDK target
`armv7a-linux-androideabi${ANDROID_API}-clang`, and Autotools host
`arm-linux-androideabi`.

```sh
export DQLITE_SRC=${DQLITE_SRC:-$PWD}
export DQLITE_ANDROID_ROOT=${DQLITE_ANDROID_ROOT:-${TMPDIR:-/tmp}/dqlite-android}
export ANDROID_API=${ANDROID_API:-24}
export ANDROID_NDK_HOME=${ANDROID_NDK_HOME:?set ANDROID_NDK_HOME first}

deps="$DQLITE_ANDROID_ROOT/armeabi-v7a/deps"
build="$DQLITE_ANDROID_ROOT/armeabi-v7a/build"

rm -rf "$deps" "$build"
mkdir -p "$deps" "$build"
cp "$DQLITE_ANDROID_ROOT/deps/conanfile.txt" "$deps/conanfile.txt"

conan install "$deps" \
  --profile:build=default \
  --output-folder="$deps/out" \
  --build=missing \
  -s:h os=Android \
  -s:h os.api_level="$ANDROID_API" \
  -s:h arch=armv7 \
  -s:h compiler=clang \
  -s:h compiler.version=18 \
  -s:h compiler.libcxx=c++_static \
  -s:h compiler.cppstd=17 \
  -s:h build_type=Release \
  -c:h tools.android:ndk_path="$ANDROID_NDK_HOME"

pcdir="$deps/out"
sed -e 's/^Name: libuv-static$/Name: libuv/' \
  "$pcdir/libuv-static.pc" > "$pcdir/libuv.pc"

tar -C "$DQLITE_SRC" \
  --exclude=.git --exclude=.deps --exclude=.libs \
  --exclude='*.o' --exclude='*.lo' --exclude='*.la' \
  -cf - . | tar -C "$build" -xf -

cd "$build"
make distclean >/dev/null 2>&1 || true
if [ ! -x ./configure ]; then
  autoreconf -i
fi

toolchain="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64"
export CC="$toolchain/bin/armv7a-linux-androideabi${ANDROID_API}-clang"
export CXX="$toolchain/bin/armv7a-linux-androideabi${ANDROID_API}-clang++"
export AR="$toolchain/bin/llvm-ar"
export RANLIB="$toolchain/bin/llvm-ranlib"
export STRIP="$toolchain/bin/llvm-strip"
export PKG_CONFIG_PATH="$pcdir"
export PKG_CONFIG_LIBDIR="$pcdir"
export CFLAGS="--sysroot=$toolchain/sysroot"
export LDFLAGS="--sysroot=$toolchain/sysroot"

./configure \
  --host=arm-linux-androideabi \
  --build=x86_64-pc-linux-gnu \
  --with-vfs-shm=portable \
  --disable-backtrace \
  --without-lz4 \
  --with-static-deps

make -j"$(nproc)" libdqlite.la
file .libs/libdqlite.a
```

Expected final artifact summary:

```text
.libs/libdqlite.a: current ar archive
```

## Android-specific implementation notes

The Android build exercises portability details that differ from Linux and
Windows:

- The portable VFS provider is required. The Linux remap provider depends on
  Linux-specific virtual memory behavior and is not used for Android.
- The Linux KAIO Raft writer backend is not used for Android. Android builds use
  the default portable LibUV writer backend.
- `aligned_alloc()` is not declared for the validated Android API level in the
  tested NDK, so Raft aligned allocations use `posix_memalign()` on non-Windows
  platforms.
- On Android armv7, `sqlite3_file` storage is only guaranteed to be 4-byte
  aligned. The portable VFS stores the per-connection cached WAL-index version
  as two `uint32_t` words so `struct vfsMainFile` does not require 8-byte
  alignment.
- Some signedness warnings are target-specific on armv7, for example around
  `socklen_t`; the code uses explicit casts where the conversion is intentional.

## Known limitations and follow-up

- The current Android validation is host cross-compilation only. It does not run
  tests on an Android emulator or device.
- Runtime smoke tests are still needed for database creation, simple read/write,
  portable VFS behavior, and TCP loopback.
- API 21 failed for `libuv/1.51.0` in the validated environment because
  `getifaddrs`/`freeifaddrs` are unavailable. API 24 is the validated minimum
  for this dependency set.
- If building from a Git checkout without a generated `configure` script,
  `autoreconf -i` is required before `./configure`.