#ifndef FLAGS_H_
#define FLAGS_H_

#include "../raft.h"

#define RAFT_DEFAULT_FEATURE_FLAGS (0)

/* Adds the flags @flags to @in and returns the new flags. Multiple flags should
 * be combined using the `|` operator. */
static inline raft_flags flagsSet(raft_flags in, raft_flags flags)
{
	return in | flags;
}

/* Clears the flags @flags from @in and returns the new flags. Multiple flags
 * should be combined using the `|` operator. */
static inline raft_flags flagsClear(raft_flags in, raft_flags flags)
{
	return in & (~flags);
}

/* Returns `true` if the single flag @flag is set in @in, otherwise returns
 * `false`. */
static inline bool flagsIsSet(raft_flags in, raft_flags flag)
{
	return (bool)(in & flag);
}

#endif /* FLAGS_H */
