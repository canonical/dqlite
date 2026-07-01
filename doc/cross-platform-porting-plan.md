# Cross-platform porting plan

This document describes the work needed to make dqlite portable across Linux,
Windows, and Android, while preserving the current Linux behavior as an
optimized backend.

The primary goal for the `cross-portable` branch should be correctness and
buildability first. Linux-specific fast paths such as KAIO, direct I/O, XFS
workarounds, and the multithreaded VFS remapping introduced by PR #747 should
become optional backends, not assumptions in common code.

## Executive summary

dqlite already uses LibUV in important places, especially for the event loop,
TCP transport, timers, async handles, and a custom thread pool. The
`cross-portable` branch has also moved several formerly Linux/POSIX-only paths
onto portable or capability-gated implementations:

- The Raft disk writer now has a backend-neutral interface. The default backend
  uses `uv_fs_write`/`uv_fs_close`, while an optional Linux-only KAIO backend can
  be selected for optimized Linux builds.
- The custom SQLite VFS has two explicit shared-memory providers: the Linux
  remap provider and the heap-backed portable provider used by Windows and
  Android builds.
- The server lifecycle now uses `uv_thread_t`, `uv_sem_t`, `uv_mutex_t`, and
  `uv_cond_t`. Persistent-state helpers still use POSIX file APIs on Unix, but
  Windows has corresponding path/open/read/rename/lock wrappers.
- The build system selects `linux-remap` only for Linux by default and allows
  non-Linux targets to build with `--with-vfs-shm=portable` without requiring
  `RWF_NOWAIT`.
- The client protocol still exposes a blocking file-descriptor-like API, but it
  now has Windows wrappers around `select`, `recv`, `send`, and `closesocket`.
- The test suite has been progressively gated or adapted for Windows/Android
  assumptions such as abstract sockets, chmod/statvfs behavior, TCP read event
  granularity, and WinSock socket handles.

The recommended strategy is to introduce explicit portability boundaries:

1. A Raft storage writer backend interface with a portable LibUV implementation
   and an optional Linux KAIO implementation.
2. A VFS shared-memory provider interface with a portable heap-backed provider
  and the current Linux remap provider kept as the optimized Linux backend.
3. A small platform layer for synchronization, filesystem state, local IPC,
   sockets, randomness, time, sorting, and tracing.
4. Capability-based tests and CI jobs for Linux optimized, Linux portable,
   Windows, and Android cross-builds.

## Portability status by area

| Area | Current implementation status | Remaining follow-up |
| --- | --- | --- |
| Build gates | `configure.ac` supports `--with-vfs-shm=auto|linux-remap|portable`; non-Linux targets can select portable mode without a hard `RWF_NOWAIT` requirement. | Keep Linux-only checks tied to Linux-only backends and add CI coverage for each supported target. |
| Raft disk writes | `src/raft/uv_writer.c` dispatches to a backend. `src/raft/uv_writer_libuv.c` is the portable default, and `src/raft/uv_writer_linux_kaio.c` is an optional Linux-only optimized backend selected with `--with-raft-io=linux-kaio` or `--with-raft-io=auto` on Linux when KAIO prerequisites are available. Linux KAIO types are not exposed in `src/raft/uv_writer.h`. | Benchmark the LibUV and KAIO backends on Linux workloads and decide whether the default should remain portable or move to auto/KAIO for Linux production builds. |
| Filesystem probing | `src/raft/uv_fs.c` still probes Linux capabilities. The portable writer does not depend on KAIO/direct I/O; the KAIO backend consumes the async/direct capability flags when selected. | Make capability reporting explicitly backend-specific to avoid confusing unused Linux capability results on portable targets. |
| VFS shared memory | `src/vfs.c` has Linux remap and portable heap-backed providers. Portable mode uses LibUV time/random/sleep helpers and `uv_mutex_t`; Android armv7 alignment is handled by the two-word local version stamp. | Continue performance work for the portable heap provider and preserve Linux remap as the optimized Linux default. |
| Server lifecycle | `src/server.h` uses `uv_thread_t`, `uv_sem_t`, `uv_cond_t`, and `uv_mutex_t`; the refresh thread uses `uv_cond_timedwait`. | Consider replacing the refresh thread with a LibUV timer for a simpler long-term design. |
| Server sockets | TCP bind now uses LibUV for TCP addresses; Unix-domain sockets and peer credential checks remain Unix-only. | Add explicit address schemes and implement Windows named-pipe local IPC if local IPC beyond TCP loopback is required. |
| Client protocol | `src/client/protocol.c` has platform wrappers for readiness/read/write/close: POSIX uses `poll`/`read`/`write`/`close`, Windows uses `select`/`recv`/`send`/`closesocket`. | A dedicated blocking stream abstraction would make the public boundary clearer, but current Windows role-management tests pass. |
| Persistent state | `src/server.c` has helper wrappers for open/read/rename/fdopen/close and uses `LockFileEx` on Windows; Unix still uses `openat`, `renameat`, `pread`, and `flock`. | A consolidated filesystem/path abstraction would reduce conditional code and improve path handling, but the current helpers are sufficient for validated Windows builds. |
| Sorting | `src/roles.c` uses native GNU/BSD `qsort_r` where available and a deterministic fallback where it is not. | No immediate blocker; keep fallback limited to small role-management arrays. |
| Random/time/tracing | Common paths now use LibUV helpers such as `uv_random`, `uv_sleep`, `uv_gettimeofday`, and platform-neutral thread-id handling. | Continue avoiding direct POSIX time/random APIs in newly portable code. |
| Tests | Windows/Wine and Android cross-compile validation are documented below; several Unix/Linux-only tests are skipped or gated on non-Unix targets. | Add automated CI jobs and emulator/device smoke tests for Android runtime coverage. |

## PR #747, the pre-747 model, and the portable provider

PR #747 enabled multithreaded execution by moving SQLite statement execution
(`sqlite3_step`) to a thread pool. The hard portability cost is not merely the
use of threads; it is the VFS technique used to preserve dqlite's WAL visibility
rules while SQLite keeps raw pointers to shared memory.

The relevant invariant is the same in all versions: after SQLite commits a
write transaction locally, dqlite must keep that transaction invisible to other
connections until the Raft apply point. SQLite makes this difficult because the
VFS `xShmMap` method returns raw WAL-index pointers which SQLite then reads and
writes directly, without calling back into the VFS for every access.

### Before PR #747: amend and invalidate the shared WAL-index

Before PR #747, dqlite used one shared WAL-index view and hid an unreplicated
transaction by editing the shared WAL-index header after `VfsPoll()`. The key
operation was to rewind fields such as `mxFrame` and `szPage` so that readers
would ignore WAL frames that had been written by SQLite but not yet applied by
Raft. The hash tables were left in place because a smaller `mxFrame` was enough
to make those entries unreachable.

That design had two advantages:

- It was conceptually portable, because it did not require replacing a memory
  mapping at a fixed virtual address.
- It copied only a small header-sized part of the WAL-index in the common case.

It also had serious drawbacks:

- The shared WAL-index itself became the synchronization point for hiding and
  revealing transactions.
- Aborted or externally applied transactions often required invalidating the
  WAL-index, forcing SQLite to rebuild it by reading the WAL.
- The approach was fragile for multithreaded readers because transaction
  visibility was controlled by mutating memory that other connections could
  read concurrently.

The pre-747 approach is therefore useful as a portability reference, but it is
not a good target for the portable provider if the SQLite thread pool remains
enabled.

### After PR #747: Linux remap provider

The PR relies on these ideas:

- SQLite's WAL-index memory pointer must remain stable after SQLite receives it
  through the VFS `xShmMap` path.
- During a write transaction, dqlite must hide unreplicated changes from other
  readers until the Raft apply point.
- On Linux, this is implemented by backing WAL-index shared memory with an
  in-memory file and using `mmap`/`mremap(MREMAP_FIXED)` to replace the mapping
  at the same virtual address with a private or shared mapping.
- `vfsRedirectShm` switches mapped regions to private mappings.
- `vfsPublishShm` copies WAL-index content back in a carefully ordered way and
  remaps the shared mapping back over the private one.
- `vfsRollbackShm` discards private changes by remapping the shared mapping
  back over the same addresses.

This is Linux-only. Windows `MapViewOfFile` cannot generally replace an
existing mapping at the same virtual address in the same way, and Android should
not rely on this kernel/glibc-specific behavior even when individual syscalls
exist.

This is the best current backend for Linux because the expensive part is handled
by the virtual-memory subsystem: dqlite swaps page-table mappings instead of
copying entire WAL-index regions. It also avoids the pre-747 pattern of hiding
transactions by repeatedly corrupting and rebuilding the shared WAL-index.

### Current portable refactoring: heap-backed per-connection views

The portable provider implemented for this branch keeps the post-747 visibility
model but replaces Linux virtual-memory remapping with explicit heap-backed
copies:

- The database owns shared heap WAL-index regions.
- Each SQLite connection receives stable heap pointers from `xShmMap`, so
  SQLite's raw pointers remain valid for the lifetime of the mapping.
- A connection synchronizes its local WAL-index view from the database shared
  regions when the shared version changes.
- When a connection takes the write lock, the provider snapshots the current
  local WAL-index state.
- `VfsPoll()` captures the local post-commit WAL-index state after SQLite has
  produced a committable transaction.
- `VfsApply()` publishes the captured state into the database shared heap
  regions and increments the shared version.
- `VfsAbort()` restores the connection's pre-transaction local view and releases
  the retained write lock.
- Checkpoint and restore paths explicitly publish or invalidate the portable
  shared heap state so that already-open connections observe the new database
  image.

This approach is intentionally closer to the PR #747 model than to the pre-747
model: it keeps unreplicated WAL-index changes private to the originating
connection instead of editing the shared WAL-index to hide them. The important
trade-off is that portability is achieved with memory copies rather than with
page-table remapping.

The provider is portable to platforms without `memfd_create`, `mmap`, or
`mremap`, and it can keep SQLite statement execution on the LibUV worker pool.
That means Linux can continue to use the optimized remap provider by default,
while Windows/macOS/Android can use the heap-backed provider without giving up
the multithreaded execution model introduced by PR #747.

### Generic portable provider optimizations

The portable provider started as a correctness-first heap/copy implementation.
The first optimization pass keeps the same generic model and avoids
platform-specific APIs, but removes avoidable locking, copying, and allocator
work:

- The shared WAL-index version is atomic, and connections check it before taking
  the shared mutex. Read-heavy workloads can therefore skip the mutex when no
  publication happened.
- Per-connection snapshot buffers are reused across write transactions instead
  of being freed and reallocated on each transaction.
- `VfsPoll()` compares the pre-transaction and post-transaction WAL-index
  regions and records which 32 KiB regions actually changed.
- `VfsApply()` and `VfsAbort()` use that dirty-region information to avoid
  copying clean regions.
- Publication avoids copying data back into the originating connection's local
  view when that view already contains the captured post-commit state.

Benchmarking with `integration-test stress/read_write` showed modest but useful
generic gains. With 16 readers and no writer, total wall time improved from
2.20765477s to 2.09417726s, a 5.14% reduction, while CPU time improved from
9.51095242s to 9.01848851s, a 5.18% reduction. With one writer and four
readers, total wall time improved from 3.59601739s to 3.49473693s, a 2.82%
reduction, while CPU time improved from 6.74843431s to 6.24131075s, a 7.51%
reduction. These numbers come from integration stress runs, not a dedicated
microbenchmark, so they should be treated as directional.

#### Reproducing the Linux baseline versus portable benchmark

The following benchmark compares a clean `main` branch build using the original
Linux storage and SHM path with the current `cross-portable` branch configured
with the portable SHM provider. It is intended to make future performance gains
or regressions easy to compare against the numbers below.

Benchmark conditions:

- Host OS: Linux, `x86_64-pc-linux-gnu`.
- Baseline branch: clean `main` worktree built with the default Linux
  `./configure` options.
- Baseline Linux capabilities observed during `configure`: `linux/io_uring.h`
  and `linux/aio_abi.h` were available.
- Baseline storage path: the original `main` `uv_writer` path still contains
  KAIO/`io_submit`/`RWF_NOWAIT` handling.
- Portable branch: `cross-portable`, configured with
  `./configure --with-vfs-shm=portable`.
- Benchmark binary: `integration-test`.
- Benchmark test: `stress/read_write`.
- Seed: `0x5eed1234`.
- Warmup: one unrecorded iteration for each branch and scenario.
- Measured iterations: `10`.
- The example commands below store logs under `$bench_root`.

Example setup and build commands:

```sh
repo_wt=$PWD
bench_root=${TMPDIR:-/tmp}/dqlite-bench-compare
main_wt=$bench_root/main
portable_wt=$bench_root/cross-portable
seed=0x5eed1234
iters=10

rm -rf "$bench_root"
mkdir -p "$bench_root"

git -C "$repo_wt" worktree add "$main_wt" main
git -C "$repo_wt" worktree add "$portable_wt" cross-portable
cd "$main_wt"
autoreconf -i > "$bench_root/main-autoreconf.log" 2>&1
./configure > "$bench_root/main-configure.log" 2>&1
make -j"$(nproc)" integration-test > "$bench_root/main-build.log" 2>&1

cd "$portable_wt"
./configure --with-vfs-shm=portable
make -j"$(nproc)" integration-test
```

Example benchmark commands, repeated once with `--iterations 1` as warmup and
then with `--iterations "$iters"` for the measured run:

```sh
cd "$main_wt"
./integration-test --seed "$seed" --no-fork --fatal-failures \
  --param writers 0 --param readers 16 --param databases 1 \
  --iterations "$iters" stress/read_write | tee "$bench_root/main-read-heavy.txt"
./integration-test --seed "$seed" --no-fork --fatal-failures \
  --param writers 1 --param readers 4 --param databases 1 \
  --iterations "$iters" stress/read_write | tee "$bench_root/main-mixed.txt"
./integration-test --seed "$seed" --no-fork --fatal-failures \
  --param writers 4 --param readers 0 --param databases 1 \
  --iterations "$iters" stress/read_write | tee "$bench_root/main-write-heavy.txt"

cd "$portable_wt"
./integration-test --seed "$seed" --no-fork --fatal-failures \
  --param writers 0 --param readers 16 --param databases 1 \
  --iterations "$iters" stress/read_write | tee "$bench_root/portable-read-heavy.txt"
./integration-test --seed "$seed" --no-fork --fatal-failures \
  --param writers 1 --param readers 4 --param databases 1 \
  --iterations "$iters" stress/read_write | tee "$bench_root/portable-mixed.txt"
./integration-test --seed "$seed" --no-fork --fatal-failures \
  --param writers 4 --param readers 0 --param databases 1 \
  --iterations "$iters" stress/read_write | tee "$bench_root/portable-write-heavy.txt"
```

Results from the recorded run:

| Scenario | `main` Linux wall | Portable wall | Wall delta | `main` Linux CPU | Portable CPU | CPU delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `writers=0 readers=16 databases=1` | 4.99435696s | 5.47747212s | +9.67% | 21.40682136s | 23.59575119s | +10.23% |
| `writers=1 readers=4 databases=1` | 10.47195579s | 7.20660453s | -31.18% | 5.59182397s | 13.70355473s | +145.06% |
| `writers=4 readers=0 databases=1` | 39.68643504s | 28.39825238s | -28.44% | 6.95202260s | 33.66797012s | +384.29% |

In this table, a positive delta means the portable branch took more time or CPU
than the Linux baseline, and a negative wall delta means the portable branch
finished faster. The read-heavy case still favors the original Linux path. The
write-heavy cases finish faster on the portable branch in wall-clock terms, but
consume substantially more CPU.

This comparison is branch-to-branch rather than a pure SHM-provider A/B test.
At the time of measurement, the portable branch also had the raft writer
refactored to `uv_fs_write`, while the `main` branch still had the original
KAIO-aware writer. Future benchmark comparisons should call out whether they are
measuring only the SHM provider or the full branch state.

#### Raft writer backend benchmark: LibUV portable versus Linux KAIO

After reintroducing the Linux KAIO writer as an optional backend, the same
branch was benchmarked twice with the portable VFS provider. This isolates the
Raft writer choice from the VFS provider choice:

- Portable writer build:
  `./configure --with-vfs-shm=portable --with-raft-io=portable`
- Linux KAIO writer build:
  `./configure --with-vfs-shm=portable --with-raft-io=linux-kaio`
- Benchmark binary: `integration-test`.
- Benchmark test: `stress/read_write`.
- Seed: `0x5eed1234`.
- Warmup: one unrecorded iteration per backend and scenario.
- Measured iterations: `10`.
- The example commands below store build and benchmark logs under
  `$bench_root`.

Measured commands followed this shape for each backend/scenario:

```sh
bench_root=${TMPDIR:-/tmp}/dqlite-bench-raftio-compare
./integration-test --seed 0x5eed1234 --no-fork --fatal-failures \
  --param writers W --param readers R --param databases D \
  --iterations 10 stress/read_write
```

Results from the recorded run:

| Scenario | Portable writer wall | Linux KAIO wall | Wall delta | Portable writer CPU | Linux KAIO CPU | CPU delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `writers=0 readers=16 databases=1` | 6.71949480s | 5.59806594s | -16.69% | 28.69448356s | 23.96964312s | -16.47% |
| `writers=1 readers=4 databases=1` | 8.03535353s | 8.91521227s | +10.95% | 14.72524030s | 4.88942354s | -66.80% |
| `writers=4 readers=0 databases=1` | 28.66580590s | 37.09324238s | +29.40% | 33.74190560s | 4.64425585s | -86.24% |

In this table, the delta is Linux KAIO relative to the portable LibUV writer.
Negative values mean KAIO was faster or used less CPU; positive wall deltas mean
KAIO took longer. In this validation run, KAIO improved the read-heavy case and
greatly reduced CPU time in writer-heavy scenarios, but it increased wall-clock
time for the mixed and write-heavy scenarios. This supports keeping the LibUV
writer as the default portable backend while retaining KAIO as an explicit Linux
optimization option that should be selected based on workload benchmarks.

#### Portable backend benchmark: Linux versus Windows/Wine

The same benchmark was also run with the Windows portable build under Wine, using
the MinGW/Conan dependency workspace and the same `libuv/1.51.0` plus
`sqlite3/3.53.3` dependency versions. This compares the Linux portable backend
against the Windows portable backend at the same branch state:

- Linux portable build:
  `./configure --with-vfs-shm=portable --with-raft-io=portable`
- Windows portable build:
  see `README_WINDOWS.md` for the MinGW/Conan setup and configure command.
- Benchmark binary: `integration-test.exe` on Windows/Wine and
  `integration-test` on Linux.
- Benchmark test: `stress/read_write`.
- Seed: `0x5eed1234`.
- Warmup: one unrecorded iteration per platform and scenario.
- Measured iterations: `10`.
- The same `$bench_root` layout can be used for the Linux and Windows/Wine logs.

The Linux benchmark used `--no-fork`. The Windows test binary used for this
validation did not accept that option, so the Windows command omitted it. CPU
time reported by the Windows/Wine run was not comparable with the Linux run, so
the table below compares wall-clock time only.

| Scenario | Linux portable wall | Windows/Wine portable wall | Wall delta |
| --- | ---: | ---: | ---: |
| `writers=0 readers=16 databases=1` | 6.71949480s | 25.02570840s | +272.43% |
| `writers=1 readers=4 databases=1` | 8.03535353s | 8.93448470s | +11.19% |
| `writers=4 readers=0 databases=1` | 28.66580590s | 9.71652050s | -66.10% |

In this table, the delta is Windows/Wine relative to Linux portable. Windows was
significantly slower in the read-heavy scenario, roughly comparable in the mixed
scenario, and faster in the write-heavy scenario in this Wine-hosted validation
run. These numbers should be treated as Wine validation data rather than native
Windows performance numbers.

The remaining follow-up optimizations are:

- Treat region 0 specially. It contains the WAL-index headers and read-mark
  synchronization fields, while later regions are mostly hash tables and can be
  copied or skipped independently.
- Improve checkpoint publication so the portable provider does not need a
  defensive full WAL materialization before truncate in edge cases where
  SQLite's checkpoint view is behind the in-memory WAL object.
- Reduce external-apply recovery by constructing or updating the shared
  WAL-index directly after `VfsApply()` when the applying connection did not
  originate the transaction. This would avoid forcing SQLite to rebuild the
  WAL-index from the WAL on the next read transaction.
- Consider optional platform-specific providers later, such as a macOS/BSD
  `mmap(MAP_FIXED)` experiment or a Windows `MapViewOfFileEx` provider, but
  keep them separate from the generic heap-backed provider.

The portable provider should remain the conservative common backend, with
performance work focused on reducing copying and recovery. The Linux remap
provider should remain the default optimized backend on Linux.

## Implemented architecture and remaining cleanup

This section describes the architecture implemented in the current
`cross-portable` branch. It intentionally avoids naming modules that do not
exist in the tree. Remaining cleanup items are described as follow-up work, not
as current structure.

### 1. Build and backend selection

The current build remains Autotools-based. Platform/backend selection is handled
with configure switches and Automake conditionals rather than a separate
`src/platform/` tree:

- `--with-vfs-shm=auto|portable|linux-remap` selects the SQLite VFS shared
  memory provider.
- `--with-raft-io=auto|portable|linux-kaio` selects the Raft storage writer
  backend.
- `VFS_SHM_LINUX_REMAP`, `VFS_SHM_PORTABLE`, and `RAFT_IO_LINUX_KAIO`
  conditionally add compile definitions and source files.

Non-Linux targets use the portable VFS and portable Raft writer paths. Linux can
also build the optional KAIO writer backend when the required Linux AIO headers
and `RWF_NOWAIT` declaration are available. There is no generic `src/platform/`
module layer in the current implementation.

### 2. Raft storage writer

`UvWriter` is implemented as a backend-neutral facade:

```c
struct UvWriterBackend {
    int (*init)(struct UvWriter *, struct uv_loop_s *, uv_file, const struct UvWriterOptions *, char *errmsg);
    int (*submit)(struct UvWriter *, struct UvWriterReq *, const uv_buf_t [], unsigned, size_t, UvWriterReqCb);
    void (*close)(struct UvWriter *, UvWriterCloseCb);
};
```

Implemented writer files:

- `src/raft/uv_writer.c`: backend selection and public facade.
- `src/raft/uv_writer_libuv.c`: portable default backend using
  `uv_fs_write`/`uv_fs_close`.
- `src/raft/uv_writer_linux_kaio.c`: optional Linux-only KAIO backend using
  `io_submit`, `eventfd`, `RWF_NOWAIT`, and threadpool fallback.
- `src/raft/uv_writer.h`: common writer API; it does not expose
  `aio_context_t`, `struct iocb`, or `struct io_event`.

Portable backend behavior:

- Queue writes sequentially by default to preserve Raft log ordering.
- Use `uv_fs_write` with async callbacks.
- Do not require direct I/O, `RWF_NOWAIT`, `eventfd`, or KAIO context creation.
- Let modern LibUV use the best available OS mechanism internally, including
  io_uring on Linux versions where LibUV enables it.

Linux KAIO backend behavior:

- KAIO/eventfd code lives in `src/raft/uv_writer_linux_kaio.c` and is compiled
  only when `--with-raft-io=linux-kaio` selects the Linux backend.
- Keep direct I/O and XFS optimizations as optional Linux accelerations.
- Compile only when headers and declarations are available.

Compared with the original KAIO implementation from `main`, the current backend
keeps the same core async flow but fixes the ownership boundary around the
`EAGAIN` fallback path. In the original code, if a completion reported
`event->res == -EAGAIN`, the request was submitted to `uv_queue_work()` while it
remained linked in the poll queue. That was workable in the normal success path,
but it made the request's ownership ambiguous during close or error handling:
the writer could still see the request as poll-managed while the threadpool had
started using it. In the current backend, the request is removed from
`poll_queue` and inserted into `work_queue` before it is submitted to the
threadpool. If `uv_queue_work()` fails, `kaioReqFinish()` remains the single
place that removes and releases the request state. This makes close semantics
match the queue that owns the in-flight operation and keeps the Linux-only KAIO
state isolated from the common writer API.

Remaining cleanup: `UvFsProbeCapabilities` still reports direct/async filesystem
capabilities independently of the selected writer backend. The current behavior
is functional and tested, but capability reporting could be made more explicitly
backend-specific.

### 3. VFS shared memory

The VFS providers are implemented inside `src/vfs.c` and selected by compile
definitions from `configure.ac`/`Makefile.am`.

Provider A: Linux remap provider

- Current `memfd_create` + `mmap` + `mremap(MREMAP_FIXED)` behavior.
- Only compiled on Linux where the required APIs exist.
- Required for the optimized Linux remapping implementation introduced by PR
  #747.

Provider B: portable heap-backed provider

- No `memfd_create`, no `mmap`, no `mremap`.
- Store database shared WAL-index regions in heap allocations.
- Return per-connection heap pointers to SQLite so each connection has a stable
  WAL-index view.
- Version the database shared WAL-index and synchronize connection-local views
  only when that version changes.
- On write lock, snapshot the connection-local WAL-index state.
- On `VfsPoll()`, capture the connection-local post-commit WAL-index state.
- On apply, publish the captured WAL-index content into the database shared heap
  buffers and increment the shared version.
- On abort, restore the connection-local pre-transaction snapshot and discard
  the captured state.
- Support SQLite statement execution from worker threads by relying on explicit
  VFS locks, connection-local WAL-index views, and versioned publication rather
  than address remapping.
- Store the per-connection cached WAL-index version as two `uint32_t` words so
  the `sqlite3_file` storage alignment contract is respected on Android armv7.

Provider C: further optimized portable provider

- Same heap/copy model as provider B, building on the current dirty-region
  tracking and atomic version fast paths with reduced checkpoint/recovery
  copying.
- Avoid depending on remapping an address that SQLite already holds.
- This may be slower than the Linux remap provider, but it is portable to
  Windows and Android.

The heap-backed provider is the fastest route to Windows/Android support while
preserving the multithreaded execution model. A single-thread-only provider is
still possible as a fallback, but it should not be the default target unless the
concurrent heap-backed provider proves too costly on a specific platform.

### 4. Synchronization and threads

Common synchronization in the portable branch uses LibUV equivalents:

- `pthread_t` -> `uv_thread_t`
- `sem_t` -> `uv_sem_t`
- `pthread_mutex_t` -> `uv_mutex_t`
- `pthread_cond_t` -> `uv_cond_t`
- C11 `mtx_t` in VFS -> `uv_mutex_t`

`src/server.h`, `src/server.c`, `src/vfs.c`, and `src/lib/threadpool.c` now use
LibUV synchronization primitives in the common portable paths. The threadpool no
longer includes `uv/unix.h` directly, and its `uv_once_t` initializer uses
zero-initialization so it compiles cleanly with both Unix pthread-backed libuv
and MinGW libuv 1.51.0 under `-Werror`.

The refresh thread in `dqlite_server` has been ported mechanically to
`uv_thread_t`/`uv_cond_t` and uses `uv_cond_timedwait` with relative timeouts.
The timer design remains a possible future cleanup because it would remove one
cross-thread path.

### 4.1. Lessons from cowsql PR #42

cowsql PR #42 is a useful precedent for this milestone. It was intentionally a
small portability step: remove an unused server semaphore and replace server
threading/synchronization primitives with LibUV equivalents. The codebase has
since diverged, so it should be treated as a pattern reference rather than a
patch to replay directly.

Patterns applied in dqlite:

- Node readiness and handover signaling use `uv_sem_t`: `uv_sem_init`,
  `uv_sem_destroy`, `uv_sem_post`, and `uv_sem_wait`.
- The node loop and server refresh thread use `uv_thread_t`, with callbacks of
  type `void (*)(void *)`.
- The refresh lock/wait pair uses `uv_mutex_t` and `uv_cond_t`.
- `pthread_cond_timedwait` absolute `CLOCK_REALTIME` deadlines were replaced by
  `uv_cond_timedwait` relative nanosecond timeouts, checking `UV_ETIMEDOUT`.
- `pthread_join` was replaced by `uv_thread_join`.

Important adjustments for dqlite and future LibUV conversions:

- Use LibUV error reporting for LibUV APIs. When a LibUV function returns an
  error code, format it with `uv_strerror`/`uv_err_name` rather than assuming
  `errno` was set by a POSIX call.
- `uv_mutex_lock`, `uv_mutex_unlock`, `uv_sem_post`, `uv_sem_wait`,
  `uv_cond_signal`, `uv_mutex_destroy`, `uv_cond_destroy`, and
  `uv_sem_destroy` return `void`, so pthread-style `rv` assertions should not be
  translated one-for-one.
- The mechanical conversion is a good first commit, but the long-term portable
  design should still consider replacing the refresh thread with a LibUV timer
  on the node loop, because that reduces cross-thread state and deadline
  conversion code.

### 5. Networking and local IPC

The Raft TCP transport under `src/raft/uv_tcp*.c` already uses LibUV TCP APIs.
The dqlite server TCP bind path now uses LibUV TCP handles as well:

- TCP listen setup uses `uv_tcp_init`, `uv_tcp_bind`, and `uv_listen`.
- Unix-domain sockets remain behind Unix-only code paths.
- Role-management and client protocol connections are portable enough for the
  validated Windows tests, with POSIX and WinSock read/write/close wrappers.

Remaining IPC follow-up:

- For local IPC, define explicit address schemes rather than overloading `@`:
  `tcp://host:port`, `unix:path`, `unix-abstract:name`, `pipe:name`.
- Treat Linux abstract sockets as Linux-only.
- Map Windows local IPC to LibUV named pipes.
- Move peer credential checks behind a platform abstraction. Windows named pipe
  authentication requires a different implementation from `SO_PEERCRED`.

The synchronous client protocol still has a file-descriptor-like public shape,
but the implementation has platform-specific readiness/read/write/close helpers.
A small blocking stream abstraction would make this boundary cleaner before
adding non-socket transports.

### 6. Filesystem, persistent state, and small compatibility helpers

There is no standalone `dq_fs` or `src/platform` filesystem module today.
Instead, `src/server.c`, `src/raft/uv_fs.c`, and `src/raft/uv_os.c` contain the
current helper boundaries:

- `src/server.c` has local wrappers for path joining, file open/read/rename,
  `fdopen`, and close. Windows uses path-based CRT calls where Unix uses
  `openat`, `renameat`, and `pread`.
- Server directory locking uses `LockFileEx` on Windows and `flock` on Unix.
- `src/raft/uv_fs.c` and `src/raft/uv_os.c` provide LibUV-based file operations
  for Raft storage paths.
- `src/lib/addr.c` parses numeric TCP addresses with LibUV IP helpers and keeps
  Unix abstract sockets behind Unix-only code.
- `src/lib/page_size.c` provides the portable page-size helper used by buffer
  code.
- `src/roles.c` keeps native GNU/BSD `qsort_r` where available and uses a local
  deterministic fallback where it is not available.
- `src/tracing.c` uses platform-specific thread-id/output helpers where needed.

Remaining cleanup: a unified filesystem/path abstraction could reduce the
amount of conditional code in `src/server.c`, but such a module is not present
in the current implementation.

### 7. Tests and CI

Current test handling and remaining CI work:

- The existing unit, integration, Raft core, and Raft UV suites run on Linux.
- Windows validation under Wine uses the portable backend, TCP loopback addresses, and
  skips or adapts tests with Unix-only assumptions.
- Android validation currently covers host cross-compilation of `libdqlite` for
  arm64-v8a and armeabi-v7a, not emulator/device runtime execution.
- Linux-optimized coverage includes the optional KAIO writer through
  `--with-raft-io=linux-kaio` and the Linux remap VFS provider through the Linux
  default VFS selection.

Remaining CI work is to automate the validated Linux and Windows/Wine commands
and add Android emulator/device smoke tests for storage, VFS, and TCP loopback.
The Linux Munit harness supports `--no-fork`; the current Windows test binary
does not accept that option, so Windows commands omit it.

## Milestones and current status

### M0: Inventory and build switches

- Add feature macros and backend selection options.
- Split Linux-only source files from common source files.
- Remove the configure-time hard failure on missing `RWF_NOWAIT` when the
  portable backend is selected.
- Add CI job for Linux portable mode even before Windows/Android work starts.

Exit criteria: Linux builds in both optimized mode and portable mode.

Status: implemented for the portable VFS selection path. Native Linux,
MinGW/Wine, and Android cross-build validation use these switches.

### M1: Portable synchronization

- Remove the unused `dqlite_node.stopped` semaphore.
- Replace `pthread_*`, `sem_t`, and C11 `mtx_t` in common code with LibUV-backed
  wrappers.
- Port or redesign the server refresh thread.
- Remove `uv/unix.h` from common includes unless gated.

Exit criteria: Linux portable mode passes existing server lifecycle tests.

Status: implemented mechanically with LibUV primitives. The remaining follow-up
is design cleanup, not a cross-build blocker.

### M2: Portable Raft storage backend

- Introduce the writer backend interface.
- Implement LibUV-backed portable writer.
- Move KAIO/eventfd/direct-I/O writer code into Linux-only files.
- Make capability probing backend-specific.
- Gate Linux KAIO tests.

Exit criteria: Raft UV unit/integration tests pass using the portable writer on
Linux; no Linux AIO headers are required for portable mode.

Status: implemented. The default writer is LibUV-backed and portable. The
optional Linux KAIO writer builds with `--with-raft-io=linux-kaio` and passes
Linux `make check`. Windows and Android builds stay on the portable backend.

### M3: Portable heap-backed VFS

- Add VFS shared-memory provider interface.
- Keep current Linux remap provider for Linux multithread mode.
- Implement portable heap-backed provider with connection-local WAL-index views
  and versioned publication.
- Keep SQLite execution threadpool enabled when the portable provider is
  selected, unless a platform-specific fallback explicitly disables it.
- Replace VFS random/time/sleep/page-size calls with platform wrappers.

Exit criteria: unit and integration VFS tests pass in Linux portable mode
without `memfd_create`, `mmap`, or `mremap` in common code.

Status: implemented and validated on Linux, Windows, and Android cross-builds.
The Linux remap provider remains available as the optimized Linux path.

### M4: Server filesystem and client stream portability

- Consolidate server state file helper wrappers if the current local wrappers
  become too hard to maintain.
- Keep `flock`/`LockFileEx` behind helper boundaries.
- Remove `poll/read/write/close` assumptions from the client protocol path or
  isolate them behind platform code.

Exit criteria: Linux portable mode no longer depends on openat/renameat/flock
in common server code.

Status: partially implemented. Windows uses wrapper helpers and `LockFileEx`;
Unix still uses `openat`, `renameat`, `pread`, and `flock` behind those helpers.

### M5: Networking and IPC portability

- Switch server listen/connect setup to LibUV TCP/pipe APIs.
- Define explicit address schemes.
- Gate Linux abstract sockets and peer credentials.
- Implement Windows named pipe or make TCP the first supported Windows local
  transport.

Exit criteria: TCP loopback integration tests pass in portable mode and do not
require Unix-domain sockets.

Status: implemented for TCP loopback. Windows integration tests use TCP loopback
addresses; Windows named-pipe local IPC remains future work.

### M6: Windows build

- Build with MinGW using portable backends.
- Keep CRT/WinSock differences isolated (`strdup`, `strndup`, `qsort_r`, file
  descriptors versus SOCKET handles).
- Run the validated Wine test suite.

Exit criteria: Windows builds libdqlite and runs the portable unit tests plus a
TCP loopback smoke test.

Status: exceeded with the current MinGW/Wine path. MSVC and CMake are not part
of the current implementation.

### M7: Android cross-build

- Use an Android NDK toolchain for cross-compilation.
- Build portable backends only.
- Avoid `/proc` AIO assumptions, abstract sockets as default, and Linux KAIO.
- Run emulator smoke tests if feasible.

Exit criteria: Android target builds libdqlite and runs portable storage/VFS/TCP
smoke tests.

Status: cross-build validated for Android arm64-v8a and armeabi-v7a at API 24.
No Android emulator/device runtime tests have been added to this repository yet.

### M8: Optional performance follow-up

- Benchmark portable LibUV writer against the optional KAIO backend and LibUV
  io_uring-enabled Linux.
- Decide whether the default should remain the portable LibUV backend or move to
  `--with-raft-io=auto` for Linux production builds.
- Optimize the portable heap-backed VFS provider for concurrent read workloads
  on Windows/Android.

## Current validation status

The `cross-portable` branch currently builds with MinGW and the portable Windows
path, Conan `libuv/1.51.0` plus `sqlite3/3.53.3`, and passes the full Windows
test set listed below under Wine:

- `integration-test.exe` non-stress: 127/127 passing, 1 skipped.
- `integration-test.exe stress/read_write`: 45/45 passing, 3 skipped.
- `unit-test.exe`: 75/75 passing, 3 skipped.
- `raft-core-unit-test.exe`: 135/135 passing.
- `raft-core-integration-test.exe`: 191/191 passing, 3 skipped.
- `raft-uv-unit-test.exe`: 20/20 passing, 61 skipped.
- `raft-uv-integration-test.exe`: 213/213 passing, 25 skipped.
- `raft-core-fuzzy-test.exe`: 57/57 passing.

The same branch also passes native Linux `make check`: 7/7 test programs
passing.

The optional Linux KAIO Raft writer backend is selected with
`--with-raft-io=linux-kaio`. It is compiled only on Linux with the required AIO
headers and `RWF_NOWAIT` declaration available, and it passes native Linux
`make check`: 7/7 test programs passing. The default Raft writer remains the
portable LibUV backend unless `--with-raft-io=linux-kaio` is explicitly selected.

Android cross-compilation was validated with Android NDK `27.2.12479018`, Conan
`libuv/1.51.0` plus `sqlite3/3.53.3`, and the portable VFS provider.
`libdqlite.la` builds successfully for:

- Android arm64-v8a, API 24.
- Android armeabi-v7a, API 24.

Android API 21 is not sufficient for the current Conan `libuv/1.51.0` recipe in
the validated NDK setup because libuv uses `getifaddrs`/`freeifaddrs`, which are not
available in that NDK API level. The build also needs the same `libuv.pc` alias
used by the MinGW setup because Conan emits `libuv-static.pc`.

Android-specific build fixes and motivations:

- Raft aligned allocation uses `posix_memalign()` on non-Windows platforms
  instead of `aligned_alloc()`. Android API 24 with the tested NDK does not
  declare `aligned_alloc()`, while `posix_memalign()` is available and has the
  same practical purpose for the aligned buffers used by Raft. Windows keeps
  `_aligned_malloc()`/`_aligned_free()` because those allocations must be freed
  with the matching CRT API.
- `socklen_t` is signed on the Android armv7 target. Comparisons against
  `sizeof(...)` and calls such as `memcmp(..., ai_addrlen)` therefore triggered
  `-Wsign-compare`/`-Wsign-conversion` under `-Werror`. The casts added in the
  address parser and TCP listen duplicate-address check make the intended type
  conversions explicit without changing runtime behavior.
- SQLite's VFS contract gives dqlite a `sqlite3_file *` buffer whose alignment
  is only guaranteed to satisfy `sqlite3_file`. On Android armv7 this is a
  32-bit pointer-sized object and is therefore only 4-byte aligned. The portable
  VFS `struct vfsMainFile` previously contained a local `uint64_t` version stamp,
  which raised the struct's required alignment to 8 bytes and made casts from
  `sqlite3_file *` unsafe on armv7. The local version stamp is now stored as two
  `uint32_t` words. The shared global WAL-index version remains an atomic
  `uint64_t`; only the per-connection cached copy was changed to respect
  SQLite's storage-alignment contract.

Recent Windows fixes covered WinSock `SOCKET` versus CRT file descriptor
handling, LibUV TCP stream wrapping, server node lifecycle cleanup across
restart, binary metadata I/O, Windows-safe test addresses, and Unix-only test
assumptions around abstract sockets, permissions, and partial TCP read event
granularity. The dependency upgrade to libuv 1.51.0 also required avoiding the
Windows `UV_ONCE_INIT` macro expansion in local code because MinGW GCC 13 reports
it as `-Wmissing-braces` under `-Werror`.

## Follow-up work

The validated portable build paths are in place. Remaining work is mainly
packaging/CI, cleanup abstractions, and runtime validation beyond
cross-compilation.

1. Keep OS-specific types out of common public headers.
2. Keep the portable LibUV writer as the default and benchmark the optional
  Linux KAIO writer before changing Linux defaults.
3. Continue optimizing the portable VFS heap-backed provider selected with
  `--with-vfs-shm=portable`.
4. Consider replacing the server refresh thread with a LibUV timer if the
  current cross-thread path becomes hard to maintain.
5. Consolidate filesystem/path helpers if the current local wrappers in
  `src/server.c` become too scattered.
6. Add first-class CI/build scripts for Windows and Android; CMake remains an
  optional future build-system improvement, not part of the current refactor.

## Known limitations and risks

| Risk | Impact | Mitigation |
| --- | --- | --- |
| VFS WAL-index semantics change subtly when replacing remap with copy/merge. | Data visibility bugs around Raft apply/abort/checkpoint. | Add focused tests for write transaction invisibility, abort, apply, checkpoint, delete-database pragma, and multiple connections. |
| Portable writer weakens durability ordering. | Raft log corruption or committed-entry loss after crash. | Keep writes serialized first; add crash/restart tests; require fsync/fdatasync after metadata-sensitive operations. |
| Windows file locking semantics differ from `flock`. | Multiple nodes may use same directory. | Implement lock-file abstraction and test double-open behavior on each platform. |
| Client protocol remains fd-like internally. | The current wrappers pass Windows role-management tests, but future local IPC or non-socket transports may need clearer ownership and blocking semantics. | Keep the existing wrappers tested and introduce a dedicated blocking stream abstraction before adding new transport kinds. |
| Android cross-build works but runtime paths fail in app sandbox. | Host cross-compilation can pass while emulator/device startup fails due to filesystem, tmpdir, or networking restrictions. | Use `uv_os_tmpdir` or caller-provided data dirs; add emulator/device smoke tests for storage, VFS, and TCP loopback. |

## Validation gaps

The current validation scope is documented above. The main validation gaps for
future work are:

- Automate the Linux, Linux KAIO, Windows/Wine, and Android cross-build checks
  in CI.
- Add Android emulator or device smoke tests for Raft storage, VFS, and TCP
  loopback.
- Add native Windows runner coverage if suitable CI capacity is available.
- Extend crash/restart and WAL-index behavior tests around the portable writer
  and portable VFS provider.