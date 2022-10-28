
# dqlite 

[![CI Tests](https://github.com/canonical/dqlite/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/canonical/dqlite/actions/workflows/build-and-test.yml) [![codecov](https://codecov.io/gh/canonical/dqlite/branch/master/graph/badge.svg)](https://codecov.io/gh/canonical/dqlite)

**注意**：中文文档有可能未及时更新，请以最新的英文[readme](./README.md)为准。

[dqlite](https://dqlite.io)是一个用C语言开发的可嵌入的，支持流复制的数据库引擎，具备高可用性和自动故障转移功能。

“dqlite”是“distributed SQLite”的简写，即分布式SQLite。意味着dqlite通过网络协议扩展SQLite，将应用程序的各个实例连接在一起，让它们作为一个高可用的集群，而不依赖外部数据库。

## 设计亮点

- 使用[libuv](https://libuv.org/)实现异步单线程的事件循环机制

- 针对SQLite 原始数据类型优化的自定义网络协议

- 基于[Raft](https://raft.github.io/)算法的数据复制及其高效[C-raft](https://github.com/canonical/raft)实现 

## license

dqlite库是在略微修改的 LGPLv3 版本下发布的，其中包括一个版权例外，允许用户在他们的项目中静态链接这个库的代码并按照自己的条款发布最终作品。如有需要，请查看完整[license](https://github.com/canonical/dqlite/blob/master/LICENSE)文件。

## 兼容性

dqlite 在 Linux 上运行，由于C-raft 的 libuv 后端的实现，需要一个支持 [native async
I/O](https://man7.org/linux/man-pages/man2/io_setup.2.html) 的内核(注意不要和[POSIX AIO](https://man7.org/linux/man-pages/man7/aio.7.html)混淆)。

## 尝试使用

查看和了解dqlite的最简单方式是使用绑定了Go dqlite的demo样例程序，Go dqlite的使用可以参考它的项目文档[relevant
documentation](https://github.com/canonical/go-dqlite#demo)。

## 视频

在 FOSDEM 2020 上有一个关于dqlite的演讲视频，您可以在[此处](https://fosdem.org/2020/schedule/event/dqlite/)观看。

## 网络协议

如果您想编写客户端，请参阅[网络协议](https://dqlite.io/docs/protocol)文档。

## 下载

如果您使用的是基于 Debian 的系统，您可以从 dqlite 的[dev PPA](https://launchpad.net/~dqlite/+archive/ubuntu/dev) 获得最新的开发版本：

```bash
sudo add-apt-repository ppa:dqlite/dev
sudo apt-get update
sudo apt-get install libdqlite-dev
```

## 源码构建

为了编译构建libdqlite，您需要准备：

- 较新版本的libuv（v1.18.0或之后的版本）

- 较新版本的sqlite3-dev

- 构建好的[C-raft](https://github.com/canonical/raft)库

您的linux发行版应该已经为您提供了预构建的 libuv 共享库和 libsqlite3-dev,就不需要在下载了，否则还需要下载这两个依赖。

编译raft库运行如下命令：

```bash
git clone https://github.com/canonical/raft.git
cd raft
autoreconf -i
./configure
make
sudo make install
cd ..
```

所有依赖的库都下载好后，运行如下命令手动编译dqlite库：

```bash
autoreconf -i
./configure
make
sudo make install
```

## 注意事项

当环境变量LIBRAFT_TRACE在启动时被设置，将启用详细跟踪。