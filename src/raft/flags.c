#include "flags.h"

inline raft_flags flagsSet(raft_flags in, raft_flags flags)
{
	return in | flags;
}

inline raft_flags flagsClear(raft_flags in, raft_flags flags)
{
	return in & (~flags);
}

inline bool flagsIsSet(raft_flags in, raft_flags flag)
{
	return (bool)(in & flag);
}
