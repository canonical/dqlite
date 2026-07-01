# Building dqlite on macOS

This document describes how to build and validate dqlite natively on macOS with
Homebrew dependencies and the portable dqlite backends.

The validated path uses the portable heap-backed SQLite VFS provider and the
default portable LibUV Raft writer. The Linux remap VFS provider and Linux KAIO
writer are Linux-only optimizations and are not used on macOS.

## Supported target

Validated target:

- OS target: macOS/Darwin
- Architecture: Apple Silicon
- Compiler: Apple Clang from Xcode command line tools
- Dependency source: Homebrew
- dqlite VFS provider: `portable`
- Raft writer backend: default portable LibUV backend
- Validation runtime: native macOS test binaries

Intel macOS should use the same backend selection, but it has not been recorded
as part of the current validation.

## Prerequisites

Install Xcode command line tools:

```sh
xcode-select --install
```

Install the equivalent Homebrew packages:

```sh
brew install autoconf automake libtool pkg-config libuv sqlite lz4
```

The examples below assume the source checkout is the current directory. They
work with the Apple Silicon Homebrew prefix `/opt/homebrew`.

## Configure

From the dqlite checkout:

```sh
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
export PKG_CONFIG_PATH="/opt/homebrew/opt/libuv/lib/pkgconfig:/opt/homebrew/opt/sqlite/lib/pkgconfig:/opt/homebrew/opt/lz4/lib/pkgconfig:/usr/local/opt/libuv/lib/pkgconfig:/usr/local/opt/sqlite/lib/pkgconfig:/usr/local/opt/lz4/lib/pkgconfig"

autoreconf -i

./configure \
  --with-vfs-shm=portable \
  --with-raft-io=portable
```

Notes:

- `--with-vfs-shm=portable` selects the heap-backed WAL-index provider. The
  Linux remap provider depends on Linux `memfd_create`, `mmap`, and `mremap`
  behavior.
- `--with-raft-io=portable` keeps the Raft disk writer on the LibUV backend.
  The KAIO writer is Linux-only.
- If you do not need LZ4 support or do not have Homebrew `lz4` installed, add
  `--without-lz4` to the configure command.

## Build

```sh
make -j"$(sysctl -n hw.ncpu)"
```

To build the validation binaries explicitly:

```sh
make -j"$(sysctl -n hw.ncpu)" \
  unit-test \
  integration-test \
  raft-core-unit-test \
  raft-core-integration-test \
  raft-uv-unit-test \
  raft-uv-integration-test \
  raft-core-fuzzy-test
```

## Validation

The current macOS validation includes a full native `make check` run with 7/7
test programs passing and no program-level skips, failures, or errors.

Run the main binaries without test forking when investigating platform-specific
failures:

```sh
./integration-test --no-fork
./raft-uv-unit-test --no-fork
./raft-uv-integration-test --no-fork
./raft-core-fuzzy-test --no-fork
```

Current recorded results:

- `make check`: 7/7 test programs passing.
- `integration-test`: 170/170 passing, 4 skipped.
- `raft-uv-unit-test`: 24/24 passing, 57 skipped.
- `raft-uv-integration-test`: 220/220 passing, 22 skipped.
- `raft-core-fuzzy-test`: 57/57 passing.

The skipped subtests are intentional platform gates for behavior that is either
Linux-specific, Windows-specific, or not reliably injectable through Darwin
dynamic libraries.

## macOS implementation notes

Apple SQLite disables or deprecates process-global auto extensions. dqlite
therefore configures the VFS open hook explicitly for internal SQLite
connections through `VfsConfigureConnection()` instead of relying only on
`sqlite3_auto_extension()`.

Darwin TCP reset reporting can differ from Linux and Windows. A write after a
reset peer may not fail on the first attempt, and the platform can surface
`SIGPIPE` before libuv reports a write error. The macOS tests use bounded retry
logic for these reset cases while preserving stricter assertions on other
platforms.

Resolver interposition tests are skipped on Darwin because libuv is typically
loaded as a dynamic library, and its internal `getaddrinfo` calls are not
reliably interposed by symbols from the test binary.

Other portability differences handled by the current tree include:

- `accept4` is unavailable, so tests use `accept` followed by `fcntl` where
  needed.
- `posix_fallocate` is unavailable, so file preallocation uses Darwin
  `F_PREALLOCATE` in test helpers.
- BSD `qsort_r` has a different callback signature from GNU `qsort_r`.
- Backtrace support is gated so Darwin builds do not depend on Linux-only
  behavior.
