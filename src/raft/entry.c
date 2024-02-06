#include <stdint.h>
#include <string.h>

#include "assert.h"
#include "entry.h"

void entryBatchesDestroy(struct raft_entry *entries, const size_t n)
{
    void *batch = NULL;
    size_t i;
    if (entries == NULL) {
        assert(n == 0);
        return;
    }
    assert(n > 0);
    for (i = 0; i < n; i++) {
        assert(entries[i].batch != NULL);
        if (entries[i].batch != batch) {
            batch = entries[i].batch;
            raft_free(batch);
        }
    }
    raft_free(entries);
}

int entryCopy(const struct raft_entry *src, struct raft_entry *dst)
{
    dst->term = src->term;
    dst->type = src->type;
    dst->buf.len = src->buf.len;
    dst->buf.base = raft_malloc(dst->buf.len);
    if (dst->buf.len > 0 && dst->buf.base == NULL) {
        return RAFT_NOMEM;
    }
    memcpy(dst->buf.base, src->buf.base, dst->buf.len);
    dst->batch = NULL;
    return 0;
}

int entryBatchCopy(const struct raft_entry *src,
                   struct raft_entry **dst,
                   const size_t n)
{
    size_t size = 0;
    void *batch;
    uint8_t *cursor;
    unsigned i;

    if (n == 0) {
        *dst = NULL;
        return 0;
    }

    /* Calculate the total size of the entries content and allocate the
     * batch. */
    for (i = 0; i < n; i++) {
        size += src[i].buf.len;
    }

    batch = raft_malloc(size);
    if (batch == NULL) {
        return RAFT_NOMEM;
    }

    /* Copy the entries. */
    *dst = raft_malloc(n * sizeof **dst);
    if (*dst == NULL) {
        raft_free(batch);
        return RAFT_NOMEM;
    }

    cursor = batch;

    for (i = 0; i < n; i++) {
        (*dst)[i].term = src[i].term;
        (*dst)[i].type = src[i].type;
        (*dst)[i].buf.base = cursor;
        (*dst)[i].buf.len = src[i].buf.len;
        (*dst)[i].batch = batch;
        memcpy((*dst)[i].buf.base, src[i].buf.base, src[i].buf.len);
        cursor += src[i].buf.len;
    }
    return 0;
}
