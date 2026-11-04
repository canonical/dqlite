# Building dqlite for Windows with MinGW

This document describes how to cross-compile dqlite for Windows from a Linux
host using MinGW-w64, Conan, and the portable dqlite backends.

The validated path targets Win32 with the MinGW-w64 POSIX thread model and uses
Wine for test execution. Native Windows execution should be validated separately
on a Windows runner before treating the port as production-ready on Windows.

## Supported target

Validated target:

- OS target: Windows, Win32
- Architecture: x86/i686
- Toolchain: MinGW-w64 GCC POSIX variant
- C compiler: `i686-w64-mingw32-gcc-posix`
- C++ compiler: `i686-w64-mingw32-g++-posix`
- Conan dependencies: static `sqlite3/3.53.3` and `libuv/1.51.0`
- dqlite VFS provider: `portable`
- Raft writer backend: default portable LibUV backend
- Validation runtime: Wine 9.0

The POSIX MinGW variant is intentional. It matches the Conan profile setting
`compiler.threads=posix` and links against the MinGW winpthreads runtime. Using
an unqualified `i686-w64-mingw32-gcc` can be ambiguous on distributions that
provide both `win32` and `posix` MinGW thread models.

You can confirm the selected thread model with:

```sh
i686-w64-mingw32-gcc-posix -v 2>&1 | grep 'Thread model'
```

## Prerequisites

Install the equivalent packages for your distribution:

- Autotools: `autoconf`, `automake`, `libtool`, `pkg-config`
- Build tools: `make`, `rsync`, `awk`, `grep`, `sed`
- MinGW-w64 POSIX toolchain for i686
- Conan 2.x
- Wine, if you want to execute the Windows test binaries on Linux

The examples below use temporary build/dependency directories outside the source
tree. They do not require committing generated Conan files to the repository.

## Prepare Conan dependencies

From the dqlite checkout:

```sh
export DQLITE_SRC=$PWD
export DQLITE_WIN_DEPS=${DQLITE_WIN_DEPS:-${TMPDIR:-/tmp}/dqlite-win32-deps}

rm -rf "$DQLITE_WIN_DEPS"
mkdir -p "$DQLITE_WIN_DEPS/profiles"
cd "$DQLITE_WIN_DEPS"
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

Create `profiles/mingw32-posix`:

```sh
cat > profiles/mingw32-posix <<'EOF'
[settings]
os=Windows
arch=x86
compiler=gcc
compiler.version=13
compiler.libcxx=libstdc++11
compiler.threads=posix
compiler.exception=dwarf2
compiler.cstd=gnu11
compiler.cppstd=gnu17
build_type=Release

[conf]
tools.build:compiler_executables={"c": "i686-w64-mingw32-gcc-posix", "cpp": "i686-w64-mingw32-g++-posix"}
tools.cmake.cmaketoolchain:generator=Ninja

[buildenv]
CC=i686-w64-mingw32-gcc-posix
CXX=i686-w64-mingw32-g++-posix
AR=i686-w64-mingw32-ar
RANLIB=i686-w64-mingw32-ranlib
STRIP=i686-w64-mingw32-strip
RC=i686-w64-mingw32-windres
WINDRES=i686-w64-mingw32-windres
EOF
```

Build/install the dependencies:

```sh
conan remote enable conancenter || true

conan install . \
  --profile:host profiles/mingw32-posix \
  --profile:build default \
  --output-folder build/mingw32-posix \
  --build=missing
```

Conan's `libuv` package emits `libuv-static.pc`. dqlite's `configure.ac` checks
for `libuv`, so create a local alias:

```sh
if [ -f build/mingw32-posix/libuv-static.pc ]; then
  sed \
    -e 's/^Name: libuv-static$/Name: libuv/' \
    -e '/^Libs:/ s/$/ -lole32/' \
    build/mingw32-posix/libuv-static.pc > build/mingw32-posix/libuv.pc
fi
```

The extra `-lole32` is required by the static Windows libuv package for
`CoTaskMemFree`.

Verify dependency discovery:

```sh
PKG_CONFIG_PATH="$DQLITE_WIN_DEPS/build/mingw32-posix" \
PKG_CONFIG_LIBDIR="$DQLITE_WIN_DEPS/build/mingw32-posix" \
pkg-config --modversion sqlite3 libuv
```

Expected versions:

```text
3.53.3
1.51.0
```

## Configure dqlite for Win32

Use a copied build tree so the Windows cross-build does not overwrite the host
Linux build artifacts:

```sh
export DQLITE_SRC=${DQLITE_SRC:-$PWD}
export DQLITE_WIN_BUILD=${DQLITE_WIN_BUILD:-${TMPDIR:-/tmp}/dqlite-win32-build}

rm -rf "$DQLITE_WIN_BUILD"
mkdir -p "$DQLITE_WIN_BUILD"
rsync -a --delete \
  --exclude='.git' \
  --exclude='.deps' \
  --exclude='.libs' \
  --exclude='*.o' \
  --exclude='*.lo' \
  --exclude='*.la' \
  "$DQLITE_SRC"/ "$DQLITE_WIN_BUILD"/

cd "$DQLITE_WIN_BUILD"
make distclean >/dev/null 2>&1 || true
autoreconf -i
```

Configure the copied tree:

```sh
PKG_CONFIG_PATH="$DQLITE_WIN_DEPS/build/mingw32-posix" \
PKG_CONFIG_LIBDIR="$DQLITE_WIN_DEPS/build/mingw32-posix" \
CC=i686-w64-mingw32-gcc-posix \
CXX=i686-w64-mingw32-g++-posix \
./configure \
  --host=i686-w64-mingw32 \
  --build=x86_64-pc-linux-gnu \
  --with-vfs-shm=portable \
  --disable-backtrace \
  --without-lz4
```

Notes:

- `--with-vfs-shm=portable` is required for Windows because the Linux remap VFS
  provider depends on Linux `memfd_create`, `mmap`, and `mremap` behavior.
- `--with-raft-io` is not specified; Windows uses the default portable LibUV
  Raft writer backend. The Linux KAIO backend is Linux-only.
- `--disable-backtrace` avoids Linux-specific backtrace dependencies.
- `--without-lz4` matches the minimal validated dependency set used for Windows.

## Build

From the configured copied tree:

```sh
make -j"$(nproc)" \
  unit-test.exe \
  integration-test.exe \
  raft-core-unit-test.exe \
  raft-core-integration-test.exe \
  raft-uv-unit-test.exe \
  raft-uv-integration-test.exe \
  raft-core-fuzzy-test.exe
```

## Wine runtime

To run the generated Windows binaries under Wine on Linux, ensure Wine can find
the MinGW runtime DLLs. On many Debian/Ubuntu-like systems, this is:

```sh
export WINEPATH='Z:\usr\i686-w64-mingw32\lib;Z:\usr\lib\gcc\i686-w64-mingw32\13-posix'
```

Adjust the paths for your distribution if the MinGW runtime DLLs are installed
elsewhere.

## Running tests with Wine

Run commands from the configured Windows build tree after exporting `WINEPATH`.

Unit tests:

```sh
wine .libs/unit-test.exe --show-stderr
```

Non-stress integration tests:

```sh
mapfile -t tests < <(
  wine .libs/integration-test.exe --list | tr -d '\r' |
  awk '$0 != "stress/read_write" {print}'
)
wine .libs/integration-test.exe --show-stderr "${tests[@]}"
```

Stress test:

```sh
wine .libs/integration-test.exe --show-stderr stress/read_write
```

Raft tests:

```sh
for exe in \
  raft-core-unit-test.exe \
  raft-core-integration-test.exe \
  raft-uv-unit-test.exe \
  raft-uv-integration-test.exe \
  raft-core-fuzzy-test.exe
do
  wine ".libs/$exe" --show-stderr
done
```

## Current validated results

The following results were observed with `sqlite3/3.53.3`, `libuv/1.51.0`,
MinGW-w64 i686 POSIX, and Wine 9.0:

| Binary | Result |
| --- | ---: |
| `integration-test.exe` non-stress | 127/127 passing, 1 skipped |
| `integration-test.exe stress/read_write` | 45/45 passing, 3 skipped |
| `unit-test.exe` | 75/75 passing, 3 skipped |
| `raft-core-unit-test.exe` | 135/135 passing |
| `raft-core-integration-test.exe` | 191/191 passing, 3 skipped |
| `raft-uv-unit-test.exe` | 20/20 passing, 61 skipped |
| `raft-uv-integration-test.exe` | 213/213 passing, 25 skipped |
| `raft-core-fuzzy-test.exe` | 57/57 passing |

## Windows-specific implementation notes

The port handles several Windows differences explicitly:

- WinSock `SOCKET` handles are not CRT file descriptors. Socket reads/writes use
  `recv`/`send`, and sockets are closed with `closesocket`.
- The client protocol uses `select` on Windows and `poll` on POSIX.
- Persistent metadata files are opened in binary mode to avoid CRT text-mode
  newline translation.
- File replacement closes metadata probe descriptors before renaming, because
  Windows rename semantics differ from POSIX.
- Server directory locking uses `LockFileEx` on Windows and `flock` on Unix.
- Unix abstract socket addresses such as `@1` are Unix-only. Windows tests use
  TCP loopback addresses.
- LibUV is preferred where it directly provides the needed semantics, for
  example TCP bind/listen, time, sleep, random, filesystem calls, threads,
  mutexes, conditions, and semaphores.

## Known limitations and follow-up

- Validation has been done under Wine. Native Windows runner validation is still
  required before treating this as native-Windows production validation.
- Windows named-pipe local IPC is not implemented. TCP loopback is the validated
  Windows transport path.
- The Wine CPU time reported by the Munit harness is not reliable for benchmark
  comparison; use wall-clock time for Wine-hosted benchmark notes.