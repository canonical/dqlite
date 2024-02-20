#ifndef RAFT_SNAPSHOT_H_
#define RAFT_SNAPSHOT_H_

#include "../raft.h"

/* Release all memory associated with the given snapshot. */
void snapshotClose(struct raft_snapshot *s);

/* Like snapshotClose(), but also release the snapshot object itself. */
void snapshotDestroy(struct raft_snapshot *s);

/* Restore a snapshot.
 *
 * This will reset the current state of the server as if the last entry
 * contained in the snapshot had just been persisted, committed and applied.
 *
 * The in-memory log must be empty when calling this function.
 *
 * If no error occurs, the memory of the snapshot object gets released. */
int snapshotRestore(struct raft *r, struct raft_snapshot *snapshot);

/* Make a full deep copy of a snapshot object.
 *
 * All data buffers in the source snapshot will be compacted in a single buffer
 * in the destination snapshot. */
int snapshotCopy(const struct raft_snapshot *src, struct raft_snapshot *dst);

#endif /* RAFT_SNAPSHOT_H */
