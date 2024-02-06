/* InstallSnapshot RPC handlers. */

#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include "../raft.h"

/* Process an InstallSnapshot RPC from the given server. */
int recvInstallSnapshot(struct raft *r,
                        raft_id id,
                        const char *address,
                        struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
