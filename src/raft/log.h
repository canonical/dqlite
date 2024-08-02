/* In-memory cache of the persistent raft log stored on disk. */

#ifndef RAFT_LOG_H_
#define RAFT_LOG_H_

#include "../raft.h"

/* Initial size of the entry reference count hash table. */
#define LOG__REFS_INITIAL_SIZE 256

/**
 * State machine for an entry in the in-memory log.
 */
enum {
	ENTRY_CREATED,
	ENTRY_COMMITTED,
	ENTRY_APPLIED,
	/* Entry failed to commit and was rolled back. */
	ENTRY_TRUNCATED,
	/* Entry was removed by installing a snapshot. */
	ENTRY_REPLACED,
	/* Entry was removed by creating a snapshot. */
	ENTRY_SNAPSHOTTED,
	ENTRY_NR,
};

/**
 * Counter for outstanding references to a log entry.
 *
 * When an entry is first appended to the log, its refcount is set to one (the
 * log itself is the only one referencing the entry). Whenever an entry is
 * included in an I/O request (to write it to disk or to send it to other
 * servers) its refcount is increased by one. Whenever an entry gets deleted
 * from the log its refcount is decreased by one. Likewise, whenever an I/O
 * request is completed the refcount of the relevant entries is decreased by
 * one. When the refcount drops to zero the memory that its @buf attribute
 * points to gets released, or, if the @batch attribute is non-NULL, a check is
 * made to see if all other entries of the same batch also have a zero refcount,
 * and the memory that @batch points to gets released if that's the case.
 */
struct raft_entry_ref
{
	raft_term term;       /* Term of the entry being ref-counted. */
	raft_index index;     /* Index of the entry being ref-counted. */
	unsigned short count; /* Number of references. */
	/* The next two fields are copied from the corresponding fields of the
	 * raft_entry pointed to by this reference. We store them here as well,
	 * so that logReinstate can retrieve them when it finds a raft_entry_ref
	 * with the same index and term as it was passed, and create a full
	 * raft_entry using them. */
	struct raft_buffer buf;
	void *batch;
	/* State machine for the tracked entry. We keep the sm here instead of
	 * adding it to raft_entry so that raft_entry can remain a stateless
	 * value type that does not necessarily represent a live entry in the
	 * in-memory log. */
	struct sm sm;
	/* Next item in the bucket (for collisions). */
	struct raft_entry_ref *next;
};

/**
 * In-memory cache of the persistent raft log stored on disk.
 *
 * The raft log cache is implemented as a circular buffer of log entries, which
 * makes some frequent operations very efficient (e.g. deleting the first N
 * entries when snapshotting).
 */
struct raft_log
{
	struct raft_entry *entries; /* Circular buffer of log entries. */
	size_t size;        /* Number of available slots in the buffer. */
	size_t front, back; /* Indexes of used slots [front, back). */
	raft_index offset;  /* Index of first entry is offset+1. */
	struct raft_entry_ref
	    *refs;        /* Log entries reference counts hash table. */
	size_t refs_size; /* Size of the reference counts hash table. */
	struct            /* Information about last snapshot, or zero. */
	{
		raft_index
		    last_index; /* Snapshot replaces all entries up to here. */
		raft_term last_term; /* Term of last index. */
	} snapshot;
};

/* Initialize an empty in-memory log of raft entries. */
struct raft_log *logInit(void);

/* Release all memory used by the given log object. */
void logClose(struct raft_log *l);

/* Called at startup when populating the log with entries loaded from disk. It
 * sets the starting state of the log. The start index must be lower or equal
 * than snapshot_index + 1. */
void logStart(struct raft_log *l,
	      raft_index snapshot_index,
	      raft_term snapshot_term,
	      raft_index start_index);

/* Get the number of entries the log currently contains. */
size_t logNumEntries(struct raft_log *l);

/* Get the index of the last entry in the log. Return #0 if the log is empty. */
raft_index logLastIndex(struct raft_log *l);

/* Get the term of the last entry in the log. Return #0 if the log is empty. */
raft_term logLastTerm(struct raft_log *l);

/* Get the term of the entry with the given index. Return #0 if @index is *
 * greater than the last index of the log, or if it's lower than oldest index we
 * know the term of (either because it's outstanding or because it's the last
 * entry in the most recent snapshot). */
raft_term logTermOf(struct raft_log *l, raft_index index);

/* Get the index of the last entry in the most recent snapshot. Return #0 if
 * there are no snapshots. */
raft_index logSnapshotIndex(struct raft_log *l);

/* Get the entry with the given index. The returned pointer remains valid only
 * as long as no API that might delete the entry with the given index is
 * invoked. Return #NULL if there is no such entry. */
const struct raft_entry *logGet(struct raft_log *l, const raft_index index);

/**
 * Retrieve a reference to the state machine for an entry, or NULL if there is
 * no such entry in the log.
 */
struct sm *log_get_entry_sm(const struct raft_log *l,
			    raft_term term,
			    raft_index index);

/* Check whether the hash map is already tracking an entry with the given
 * @term and @index (that is not part of the "logical" log). If so, increment
 * the refcount of that entry and set @reinstated to true; otherwise, set
 * @reinstated to false. */
int logReinstate(struct raft_log *l,
		 raft_term term,
		 unsigned short type,
		 bool *reinstated);

/* Append a new entry to the log. */
int logAppend(struct raft_log *l,
	      raft_term term,
	      unsigned short type,
	      struct raft_buffer buf,
	      struct raft_entry_local_data local_data,
	      bool is_local,
	      void *batch);

/* Convenience to encode and append a single #RAFT_CHANGE entry. */
int logAppendConfiguration(struct raft_log *l,
			   const raft_term term,
			   const struct raft_configuration *configuration);

/* Acquire an array of entries from the given index onwards. The payload
 * memory referenced by the @buf attribute of the returned entries is guaranteed
 * to be valid until logRelease() is called. */
int logAcquire(struct raft_log *l,
	       raft_index index,
	       struct raft_entry *entries[],
	       unsigned *n);

/* Release a previously acquired array of entries. */
void logRelease(struct raft_log *l,
		raft_index index,
		struct raft_entry entries[],
		unsigned n);

/* Delete all entries from the given index (included) onwards. If the log is
 * empty this is a no-op. If @index is lower than or equal to the index of the
 * first entry in the log, then the log will become empty. */
void logTruncate(struct raft_log *l, const raft_index index);

/* Discard all entries from the given index (included) onwards. This is exactly
 * the same as truncate, but the memory of the entries does not gets
 * released. This is called as part of error handling, when reverting the effect
 * of previous logAppend calls. */
void logDiscard(struct raft_log *l, const raft_index index);

/* To be called when taking a new snapshot. The log must contain an entry at
 * last_index, which is the index of the last entry included in the
 * snapshot. The function will update the last snapshot information and delete
 * all entries up to last_index - trailing (included). If the log contains no
 * entry at last_index - trailing, then no entry will be deleted. */
void logSnapshot(struct raft_log *l, raft_index last_index, unsigned trailing);

/* To be called when installing a snapshot.
 *
 * The log can be in any state. All outstanding entries will be discarded, the
 * last index and last term of the most recent snapshot will be set to the given
 * values, and the offset adjusted accordingly. */
void logRestore(struct raft_log *l, raft_index last_index, raft_term last_term);

#endif /* RAFT_LOG_H_ */
