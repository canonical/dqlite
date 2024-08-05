#include "log.h"

#include <string.h>

#include "../raft.h"
#include "../utils.h" /* PRE, POST */
#include "assert.h"
#include "configuration.h"

static struct sm_conf entry_states[ENTRY_NR] = {
	[ENTRY_CREATED] = {
		.name = "created",
		/* Note: the inclusion of SNAPSHOTTED here is a concession to
		 * practicality. Removing it causes some tests to fail because
		 * they manipulate the log directly and have not been updated
		 * to perform the CREATED -> COMMITTED -> APPLIED transition
		 * (as replicationApply does). Also, the entry at the very
		 * beginning of the log doesn't go through replicationApply in
		 * all cases. */
		.allowed = BITS(ENTRY_TRUNCATED)
			  |BITS(ENTRY_COMMITTED)
			  |BITS(ENTRY_REPLACED)
			  |BITS(ENTRY_SNAPSHOTTED),
		.flags = SM_INITIAL|SM_FINAL,
	},
	[ENTRY_COMMITTED] = {
		.name = "committed",
		.allowed = BITS(ENTRY_COMMITTED)
			  |BITS(ENTRY_APPLIED),
		.flags = SM_FINAL,
	},
	[ENTRY_APPLIED] = {
		.name = "applied",
		.allowed = BITS(ENTRY_REPLACED)
			  |BITS(ENTRY_SNAPSHOTTED),
		.flags = SM_FINAL,
	},
	[ENTRY_TRUNCATED] = {
		.name = "truncated",
		.flags = SM_FINAL,
	},
	[ENTRY_REPLACED] = {
		.name = "replaced",
		.flags = SM_FINAL,
	},
	[ENTRY_SNAPSHOTTED] = {
		.name = "snapshotted",
		.flags = SM_FINAL,
	},
};

static bool entry_invariant(const struct sm *sm, int prev)
{
	(void)sm;
	(void)prev;
	return true;
}

/* Calculate the reference count hash table key for the given log entry index in
 * an hash table of the given size.
 *
 * The hash is simply the log entry index minus one modulo the size. This
 * minimizes conflicts in the most frequent case, where a new log entry is
 * simply appended to the log and can use the hash table bucket next to the
 * bucket for the entry with the previous index (possibly resizing the table if
 * its cap is reached). */
static size_t refsKey(const raft_index index, const size_t size)
{
	assert(index > 0);
	assert(size > 0);
	return (size_t)((index - 1) % size);
}

/* Try to insert a new reference count item for the given log entry index into
 * the given reference count hash table.
 *
 * A collision happens when the bucket associated with the hash key of the given
 * log entry index is already used to refcount log entries with a different
 * index. In that case the collision output parameter will be set to true and no
 * new reference count item is inserted into the hash table.
 *
 * If two log entries have the same index but different terms, the associated
 * bucket will be grown accordingly.
 *
 * Returns a pointer to the new reference object on success, or NULL if a
 * memory allocation failed. */
static struct raft_entry_ref *refsTryInsert(struct raft_entry_ref *table,
					    const size_t size,
					    const raft_term term,
					    const raft_index index,
					    const unsigned short count,
					    struct raft_buffer buf,
					    void *batch,
					    bool *collision)
{
	struct raft_entry_ref *bucket; /* Bucket associated with this index. */
	struct raft_entry_ref *next_slot; /* For traversing the bucket slots. */
	struct raft_entry_ref
	    *last_slot;              /* To track the last traversed slot. */
	struct raft_entry_ref *slot; /* Actual slot to use for this entry. */
	size_t key;

	assert(table != NULL);
	assert(size > 0);
	assert(term > 0);
	assert(index > 0);
	assert(count > 0);
	assert(collision != NULL);

	/* Calculate the hash table key for the given index. */
	key = refsKey(index, size);
	bucket = &table[key];

	/* If a bucket is empty, then there's no collision and we can fill its
	 * first slot. */
	if (bucket->count == 0) {
		assert(bucket->next == NULL);
		slot = bucket;
		goto fill;
	}

	/* If the bucket is already used to refcount entries with a different
	 * index, then we have a collision and we must abort here. */
	if (bucket->index != index) {
		*collision = true;
		return NULL;
	}

	/* If we get here it means that the bucket is in use to refcount one or
	 * more entries with the same index as the given one, but different
	 * terms.
	 *
	 * We must append a newly allocated slot to refcount the entry with this
	 * term.
	 *
	 * So first let's find the last slot in the bucket. */
	for (next_slot = bucket; next_slot != NULL;
	     next_slot = next_slot->next) {
		/* All entries in a bucket must have the same index. */
		assert(next_slot->index == index);

		/* It should never happen that two entries with the same index
		 * and term get appended. So no existing slot in this bucket
		 * must track an entry with the same term as the given one. */
		assert(next_slot->term != term);

		last_slot = next_slot;
	}

	/* The last slot must have no next slot. */
	assert(last_slot->next == NULL);

	slot = raft_malloc(sizeof *slot);
	if (slot == NULL) {
		return NULL;
	}

	last_slot->next = slot;

fill:
	slot->term = term;
	slot->index = index;
	slot->count = count;
	slot->buf = buf;
	slot->batch = batch;
	slot->next = NULL;

	*collision = false;

	return slot;
}

/* Move the slots of the given bucket into the given reference count hash
 * table. The key of the bucket to use in the given table will be re-calculated
 * according to the given size. */
static int refsMove(struct raft_entry_ref *bucket,
		    struct raft_entry_ref *table,
		    const size_t size)
{
	struct raft_entry_ref *slot;
	struct raft_entry_ref *next_slot;

	assert(bucket != NULL);
	assert(table != NULL);
	assert(size > 0);

	/* Only non-empty buckets should be moved. */
	assert(bucket->count > 0);

	/* For each slot in the bucket, insert the relevant entry in the given
	 * table, then free it. */
	next_slot = bucket;
	while (next_slot != NULL) {
		struct raft_entry_ref *ref;
		struct sm sm;
		bool collision;

		slot = next_slot;

		/* Insert the reference count for this entry into the new table.
		 */
		ref = refsTryInsert(table, size, slot->term, slot->index,
				    slot->count, slot->buf, slot->batch,
				    &collision);

		next_slot = slot->next;
		sm = slot->sm;
		/* Unless this is the very first slot in the bucket, we need to
		 * free the slot. */
		if (slot != bucket) {
			raft_free(slot);
		}
		if (ref != NULL) {
			ref->sm = sm;
		} else {
			return RAFT_NOMEM;
		}

		/* The given hash table is assumed to be large enough to hold
		 * all ref counts without any conflict. */
		assert(!collision);
	};

	return 0;
}

/* Grow the size of the reference count hash table. */
static int refsGrow(struct raft_log *l)
{
	struct raft_entry_ref *table; /* New hash table. */
	size_t size;                  /* Size of the new hash table. */
	size_t i;

	assert(l != NULL);
	assert(l->refs_size > 0);

	size = l->refs_size * 2; /* Double the table size */

	table = raft_calloc(size, sizeof *table);
	if (table == NULL) {
		return RAFT_NOMEM;
	}

	/* Populate the new hash table, inserting all entries existing in the
	 * current hash table. Each bucket will have a different key in the new
	 * hash table, since the size has changed. */
	for (i = 0; i < l->refs_size; i++) {
		struct raft_entry_ref *bucket = &l->refs[i];
		if (bucket->count > 0) {
			int rv = refsMove(bucket, table, size);
			if (rv != 0) {
				return rv;
			}
		} else {
			/* If the count is zero, we expect that the bucket is
			 * unused. */
			assert(bucket->next == NULL);
		}
	}

	raft_free(l->refs);

	l->refs = table;
	l->refs_size = size;

	return 0;
}

/* Initialize the reference count of the entry with the given index, setting it
 * to 1. */
static struct raft_entry_ref *refsInit(struct raft_log *l,
				       const raft_term term,
				       const raft_index index,
				       struct raft_buffer buf,
				       void *batch)
{
	int i;

	assert(l != NULL);
	assert(term > 0);
	assert(index > 0);

	/* Initialize the hash map with a reasonable size */
	if (l->refs == NULL) {
		l->refs_size = LOG__REFS_INITIAL_SIZE;
		l->refs = raft_calloc(l->refs_size, sizeof *l->refs);
		if (l->refs == NULL) {
			return NULL;
		}
	}

	/* Check if the bucket associated with the given index is available
	 * (i.e. there are no collisions), or grow the table and re-key it
	 * otherwise.
	 *
	 * We limit the number of times we try to grow the table to 10, to avoid
	 * eating up too much memory. In practice, there should never be a case
	 * where this is not enough. */
	for (i = 0; i < 10; i++) {
		bool collision;
		struct raft_entry_ref *ref;
		int rc;

		ref = refsTryInsert(l->refs, l->refs_size, term, index, 1, buf,
				    batch, &collision);
		if (ref == NULL && !collision) {
			return NULL;
		} else if (!collision) {
			sm_init(&ref->sm, entry_invariant, NULL, entry_states,
				"entry", ENTRY_CREATED);
			return ref;
		}

		rc = refsGrow(l);
		if (rc != 0) {
			assert(rc == RAFT_NOMEM);
			return NULL;
		}
	};

	return NULL;
}

/* Lookup the slot associated with the given term/index, which must have
 * been previously inserted. */
static struct raft_entry_ref *refs_get(const struct raft_log *l,
		     const raft_term term,
		     const raft_index index)
{
	assert(l != NULL);
	assert(term > 0);
	assert(index > 0);

	size_t key = refsKey(index, l->refs_size);
	struct raft_entry_ref *slot = &l->refs[key];
	while (1) {
		PRE(slot != NULL);
		assert(slot->index == index);
		if (slot->term == term) {
			break;
		}
		slot = slot->next;
	}
	POST(slot != NULL);
	return slot;
}

/* Increment the refcount of the entry with the given term and index. */
static void refsIncr(struct raft_log *l,
		     const raft_term term,
		     const raft_index index)
{
	struct raft_entry_ref *slot = refs_get(l, term, index);
	slot->count++;
}

/* Decrement the refcount of the entry with the given index. Return a boolean
 * indicating whether the entry has now zero references.
 *
 * Also moves the entry's sm if a valid (nonnegative) state is provided. */
static bool refsDecr(struct raft_log *l,
		     const raft_term term,
		     const raft_index index,
		     int state)
{
	size_t key;                  /* Hash table key for the given index. */
	struct raft_entry_ref *slot; /* Slot for the given term/index */
	struct raft_entry_ref
	    *prev_slot; /* Slot preceeding the one to decrement */

	assert(l != NULL);
	assert(term > 0);
	assert(index > 0);

	key = refsKey(index, l->refs_size);
	prev_slot = NULL;

	/* Lookup the slot associated with the given term/index, keeping track
	 * of its previous slot in the bucket list. */
	slot = &l->refs[key];
	while (1) {
		assert(slot != NULL);
		assert(slot->index == index);
		if (slot->term == term) {
			break;
		}
		prev_slot = slot;
		slot = slot->next;
	}

	slot->count--;
	if (state >= 0) {
		sm_move(&slot->sm, state);
	}

	if (slot->count > 0) {
		/* The entry is still referenced. */
		return false;
	}

	/* If the refcount has dropped to zero, delete the slot. */
	sm_fini(&slot->sm);
	if (prev_slot != NULL) {
		/* This isn't the very first slot, simply unlink it from the
		 * slot list. */
		prev_slot->next = slot->next;
		raft_free(slot);
	} else if (slot->next != NULL) {
		/* This is the very first slot, and slot list is not empty. Copy
		 * the second slot into the first one, then delete it. */
		struct raft_entry_ref *second_slot = slot->next;
		*slot = *second_slot;
		raft_free(second_slot);
	}

	return true;
}

struct raft_log *logInit(void)
{
	struct raft_log *log;

	log = raft_malloc(sizeof(*log));
	if (log == NULL) {
		return NULL;
	}

	log->entries = NULL;
	log->size = 0;
	log->front = log->back = 0;
	log->offset = 0;
	log->refs = NULL;
	log->refs_size = 0;
	log->snapshot.last_index = 0;
	log->snapshot.last_term = 0;

	return log;
}

/* Return the index of the i'th entry in the log. */
static raft_index indexAt(struct raft_log *l, size_t i)
{
	return l->offset + i + 1;
}

/* Return the circular buffer position of the i'th entry in the log. */
static size_t positionAt(struct raft_log *l, size_t i)
{
	return (l->front + i) % l->size;
}

/* Return the i'th entry in the log. */
static struct raft_entry *entryAt(struct raft_log *l, size_t i)
{
	return &l->entries[positionAt(l, i)];
}

void logClose(struct raft_log *l)
{
	void *batch = NULL; /* Last batch that has been freed */

	assert(l != NULL);

	if (l->entries != NULL) {
		size_t i;
		size_t n = logNumEntries(l);

		for (i = 0; i < n; i++) {
			struct raft_entry *entry = entryAt(l, i);
			raft_index index = indexAt(l, i);
			size_t key = refsKey(index, l->refs_size);
			struct raft_entry_ref *slot = &l->refs[key];

			/* We require that there are no outstanding references
			 * to active entries. */
			assert(slot->count == 1);
			sm_fini(&slot->sm);

			/* TODO: we should support the case where the bucket has
			 * more than one slot. */
			assert(slot->next == NULL);

			/* Release the memory used by the entry data (either
			 * directly or via a batch). */
			if (entry->batch == NULL) {
				if (entry->buf.base != NULL) {
					raft_free(entry->buf.base);
				}
			} else {
				if (entry->batch != batch) {
					/* This batch was not released yet, so
					 * let's do it now. */
					batch = entry->batch;
					raft_free(entry->batch);
				}
			}
		}

		raft_free(l->entries);
	}

	if (l->refs != NULL) {
		raft_free(l->refs);
	}

	raft_free(l);
}

void logStart(struct raft_log *l,
	      raft_index snapshot_index,
	      raft_term snapshot_term,
	      raft_index start_index)
{
	assert(logNumEntries(l) == 0);
	assert(start_index > 0);
	assert(start_index <= snapshot_index + 1);
	assert(snapshot_index == 0 || snapshot_term != 0);
	l->snapshot.last_index = snapshot_index;
	l->snapshot.last_term = snapshot_term;
	l->offset = start_index - 1;
}

/* Ensure that the entries array has enough free slots for adding a new entry.
 */
static int ensureCapacity(struct raft_log *l)
{
	struct raft_entry *entries; /* New entries array */
	size_t n;                   /* Current number of entries */
	size_t size;                /* Size of the new array */
	size_t i;

	n = logNumEntries(l);

	if (n + 1 < l->size) {
		return 0;
	}

	/* Make the new size twice the current size plus one (for the new
	 * entry). Over-allocating now avoids smaller allocations later. */
	size = (l->size + 1) * 2;

	entries = raft_calloc(size, sizeof *entries);
	if (entries == NULL) {
		return RAFT_NOMEM;
	}

	/* Copy all active old entries to the beginning of the newly allocated
	 * array. */
	for (i = 0; i < n; i++) {
		memcpy(&entries[i], entryAt(l, i), sizeof *entries);
	}

	/* Release the old entries array. */
	if (l->entries != NULL) {
		raft_free(l->entries);
	}

	l->entries = entries;
	l->size = size;
	l->front = 0;
	l->back = n;

	return 0;
}

int logReinstate(struct raft_log *l,
		 raft_term term,
		 unsigned short type,
		 bool *reinstated)
{
	raft_index index;
	size_t key;
	struct raft_entry_ref *bucket;
	struct raft_entry_ref *slot;
	struct raft_entry *entry;
	int rv;

	*reinstated = false;

	if (l->refs_size == 0) {
		return 0;
	}

	index = logLastIndex(l) + 1;
	key = refsKey(index, l->refs_size);
	bucket = &l->refs[key];
	if (bucket->count == 0 || bucket->index != index) {
		return 0;
	}

	for (slot = bucket; slot != NULL; slot = slot->next) {
		if (slot->term == term) {
			rv = ensureCapacity(l);
			if (rv != 0) {
				return rv;
			}
			slot->count++;
			l->back++;
			l->back %= l->size;
			entry = &l->entries[l->back];
			entry->term = term;
			entry->type = type;
			entry->buf = slot->buf;
			entry->batch = slot->batch;
			*reinstated = true;
			break;
		}
	}

	return 0;
}

int logAppend(struct raft_log *l,
	      const raft_term term,
	      const unsigned short type,
	      struct raft_buffer buf,
	      struct raft_entry_local_data local_data,
	      bool is_local,
	      void *batch)
{
	int rv;
	struct raft_entry *entry;
	struct raft_entry_ref *ref;
	raft_index index;

	assert(l != NULL);
	assert(term > 0);
	assert(type == RAFT_CHANGE || type == RAFT_BARRIER ||
	       type == RAFT_COMMAND);

	rv = ensureCapacity(l);
	if (rv != 0) {
		return rv;
	}

	index = logLastIndex(l) + 1;

	ref = refsInit(l, term, index, buf, batch);
	if (ref == NULL) {
		return RAFT_NOMEM;
	}

	entry = &l->entries[l->back];
	entry->term = term;
	entry->type = type;
	entry->buf = buf;
	entry->batch = batch;
	entry->local_data = local_data;
	entry->is_local = is_local;

	l->back += 1;
	l->back = l->back % l->size;

	return 0;
}

int logAppendConfiguration(struct raft_log *l,
			   const raft_term term,
			   const struct raft_configuration *configuration)
{
	struct raft_buffer buf;
	int rv;

	assert(l != NULL);
	assert(term > 0);
	assert(configuration != NULL);

	/* Encode the configuration into a buffer. */
	rv = configurationEncode(configuration, &buf);
	if (rv != 0) {
		goto err;
	}

	/* Append the new entry to the log. */
	rv = logAppend(l, term, RAFT_CHANGE, buf, (struct raft_entry_local_data){}, true, NULL);
	if (rv != 0) {
		goto err_after_encode;
	}

	return 0;

err_after_encode:
	raft_free(buf.base);

err:
	assert(rv != 0);
	return rv;
}

size_t logNumEntries(struct raft_log *l)
{
	assert(l != NULL);

	/* The circular buffer is not wrapped. */
	if (l->front <= l->back) {
		return l->back - l->front;
	}

	/* The circular buffer is wrapped. */
	return l->size - l->front + l->back;
}

raft_index logLastIndex(struct raft_log *l)
{
	/* If there are no entries in the log, but there is a snapshot available
	 * check that it's last index is consistent with the offset. */
	if (logNumEntries(l) == 0 && l->snapshot.last_index != 0) {
		assert(l->offset <= l->snapshot.last_index);
	}
	return l->offset + logNumEntries(l);
}

/* Return the position of the entry with the given index in the entries array.
 *
 * If no entry with the given index is in the log return the size of the entries
 * array. */
static size_t locateEntry(struct raft_log *l, const raft_index index)
{
	size_t n = logNumEntries(l);

	if (n == 0 || index < indexAt(l, 0) || index > indexAt(l, n - 1)) {
		return l->size;
	}

	/* Get the circular buffer position of the desired entry. Log indexes
	 * start at 1, so we subtract one to get array indexes. We also need to
	 * subtract any index offset this log might start at. */
	return positionAt(l, (size_t)((index - 1) - l->offset));
}

raft_term logTermOf(struct raft_log *l, const raft_index index)
{
	size_t i;
	assert(index > 0);
	assert(l->offset <= l->snapshot.last_index);

	if ((index < l->offset + 1 && index != l->snapshot.last_index) ||
	    index > logLastIndex(l)) {
		return 0;
	}

	if (index == l->snapshot.last_index) {
		assert(l->snapshot.last_term != 0);
		/* Coherence check that if we still have the entry at
		 * last_index, its term matches the one in the snapshot. */
		i = locateEntry(l, index);
		if (i != l->size) {
			assert(l->entries[i].term == l->snapshot.last_term);
		}
		return l->snapshot.last_term;
	}

	i = locateEntry(l, index);
	assert(i < l->size);
	return l->entries[i].term;
}

raft_index logSnapshotIndex(struct raft_log *l)
{
	return l->snapshot.last_index;
}

raft_term logLastTerm(struct raft_log *l)
{
	raft_index last_index;
	last_index = logLastIndex(l);
	return last_index > 0 ? logTermOf(l, last_index) : 0;
}

const struct raft_entry *logGet(struct raft_log *l, const raft_index index)
{
	size_t i;

	assert(l != NULL);

	/* Get the array index of the desired entry. */
	i = locateEntry(l, index);
	if (i == l->size) {
		return NULL;
	}

	assert(i < l->size);

	return &l->entries[i];
}

struct sm *log_get_entry_sm(const struct raft_log *l,
			    raft_term term,
			    raft_index index)
{
	struct raft_entry_ref *slot = refs_get(l, term, index);
	return &slot->sm;
}

int logAcquire(struct raft_log *l,
	       const raft_index index,
	       struct raft_entry *entries[],
	       unsigned *n)
{
	size_t i;
	size_t j;

	assert(l != NULL);
	assert(index > 0);
	assert(entries != NULL);
	assert(n != NULL);

	/* Get the array index of the first entry to acquire. */
	i = locateEntry(l, index);

	if (i == l->size) {
		*n = 0;
		*entries = NULL;
		return 0;
	}

	if (i < l->back) {
		/* The last entry does not wrap with respect to i, so the number
		 * of entries is simply the length of the range [i...l->back).
		 */
		*n = (unsigned)(l->back - i);
	} else {
		/* The last entry wraps with respect to i, so the number of
		 * entries is the sum of the lengths of the ranges [i...l->size)
		 * and [0...l->back), which is l->size - i + l->back.*/
		*n = (unsigned)(l->size - i + l->back);
	}

	assert(*n > 0);

	*entries = raft_calloc(*n, sizeof **entries);
	if (*entries == NULL) {
		return RAFT_NOMEM;
	}

	for (j = 0; j < *n; j++) {
		size_t k = (i + j) % l->size;
		struct raft_entry *entry = &(*entries)[j];
		*entry = l->entries[k];
		refsIncr(l, entry->term, index + j);
	}

	return 0;
}

/* Return true if the given batch is referenced by any entry currently in the
 * log. */
static bool isBatchReferenced(struct raft_log *l, const void *batch)
{
	size_t i;

	/* Iterate through all live entries to see if there's one
	 * belonging to the same batch. This is slightly inefficient but
	 * this code path should be taken very rarely in practice. */
	for (i = 0; i < logNumEntries(l); i++) {
		struct raft_entry *entry = entryAt(l, i);
		if (entry->batch == batch) {
			return true;
		}
	}

	return false;
}

void logRelease(struct raft_log *l,
		const raft_index index,
		struct raft_entry entries[],
		const unsigned n)
{
	size_t i;
	void *batch = NULL; /* Last batch whose memory was freed */

	assert(l != NULL);
	assert((entries == NULL && n == 0) || (entries != NULL && n > 0));

	for (i = 0; i < n; i++) {
		struct raft_entry *entry = &entries[i];
		bool unref;

		unref = refsDecr(l, entry->term, index + i, -1);

		/* If there are no outstanding references to this entry, free
		 * its payload if it's not part of a batch, or check if we can
		 * free the batch itself. */
		if (unref) {
			if (entries[i].batch == NULL) {
				if (entry->buf.base != NULL) {
					raft_free(entries[i].buf.base);
				}
			} else {
				if (entry->batch != batch) {
					if (!isBatchReferenced(l,
							       entry->batch)) {
						batch = entry->batch;
						raft_free(batch);
					}
				}
			}
		}
	}

	if (entries != NULL) {
		raft_free(entries);
	}
}

/* Clear the log if it became empty. */
static void clearIfEmpty(struct raft_log *l)
{
	if (logNumEntries(l) > 0) {
		return;
	}
	raft_free(l->entries);
	l->entries = NULL;
	l->size = 0;
	l->front = 0;
	l->back = 0;
}

/* Destroy an entry, possibly releasing the memory of its buffer. */
static void destroyEntry(struct raft_log *l, struct raft_entry *entry)
{
	if (entry->batch == NULL) {
		if (entry->buf.base != NULL) {
			raft_free(entry->buf.base);
		}
	} else {
		if (!isBatchReferenced(l, entry->batch)) {
			raft_free(entry->batch);
		}
	}
}

/* Core logic of @logTruncate and @logDiscard, removing all log entries from
 * @index onward. If @destroy is true, also destroy the removed entries. */
static void removeSuffix(struct raft_log *l,
			 const raft_index index,
			 bool destroy,
			 int state)
{
	size_t i;
	size_t n;
	raft_index start = index;

	assert(l != NULL);
	assert(index > l->offset);
	assert(index <= logLastIndex(l));

	/* Number of entries to delete */
	n = (size_t)(logLastIndex(l) - start) + 1;

	for (i = 0; i < n; i++) {
		struct raft_entry *entry;
		bool unref;

		if (l->back == 0) {
			l->back = l->size - 1;
		} else {
			l->back--;
		}

		entry = &l->entries[l->back];
		unref = refsDecr(l, entry->term, start + n - i - 1, state);

		if (unref && destroy) {
			destroyEntry(l, entry);
		}
	}

	clearIfEmpty(l);
}

void logTruncate(struct raft_log *l, const raft_index index)
{
	if (logNumEntries(l) == 0) {
		return;
	}
	removeSuffix(l, index, true, ENTRY_TRUNCATED);
}

void logDiscard(struct raft_log *l, const raft_index index)
{
	removeSuffix(l, index, false, ENTRY_TRUNCATED);
}

/* Delete all entries up to the given index (included). */
static void removePrefix(struct raft_log *l, const raft_index index)
{
	size_t i;
	size_t n;

	assert(l != NULL);
	assert(index > 0);
	assert(index <= logLastIndex(l));

	/* Number of entries to delete */
	n = (size_t)(index - indexAt(l, 0)) + 1;

	for (i = 0; i < n; i++) {
		struct raft_entry *entry;
		bool unref;

		entry = &l->entries[l->front];

		if (l->front == l->size - 1) {
			l->front = 0;
		} else {
			l->front++;
		}
		l->offset++;

		unref = refsDecr(l, entry->term, l->offset, ENTRY_SNAPSHOTTED);

		if (unref) {
			destroyEntry(l, entry);
		}
	}

	clearIfEmpty(l);
}

void logSnapshot(struct raft_log *l, raft_index last_index, unsigned trailing)
{
	raft_term last_term = logTermOf(l, last_index);

	/* We must have an entry at this index */
	assert(last_term != 0);

	l->snapshot.last_index = last_index;
	l->snapshot.last_term = last_term;

	/* If we have not at least n entries preceeding the given last index,
	 * then there's nothing to remove and we're done. */
	if (last_index <= trailing ||
	    locateEntry(l, last_index - trailing) == l->size) {
		return;
	}

	removePrefix(l, last_index - trailing);
}

void logRestore(struct raft_log *l, raft_index last_index, raft_term last_term)
{
	size_t n = logNumEntries(l);
	assert(last_index > 0);
	assert(last_term > 0);
	if (n > 0) {
		removeSuffix(l, logLastIndex(l) - n + 1, true, ENTRY_REPLACED);
	}
	l->snapshot.last_index = last_index;
	l->snapshot.last_term = last_term;
	l->offset = last_index;
}
