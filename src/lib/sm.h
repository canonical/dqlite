#ifndef __LIB_SM__
#define __LIB_SM__

#include <stdint.h>
#include <stdbool.h>

#define BITS(state) (1ULL << (state))

enum {
	SM_PREV_NONE  = -1,
	/* sizeof(sm_conf::allowed * 8) */
	SM_STATES_MAX = 64,
	/* flags */
	SM_INITIAL    = 1U << 0,
	SM_FAILURE    = 1U << 1,
	SM_FINAL      = 1U << 2,
};

struct sm_conf {
	uint32_t    flags;
	uint64_t    allowed;
	const char *name;
};

struct sm {
	int    rc;
	int    state;
	bool (*is_locked)(const struct sm *);
	bool (*invariant)(const struct sm *, int);
	const struct sm_conf *conf;
};

void sm_init(struct sm *m,
	     bool (*invariant)(const struct sm *, int),
	     bool (*is_locked)(const struct sm *),
	     const struct sm_conf *conf,
	     int state);
void sm_fini(struct sm *m);
void sm_move(struct sm *m, int next_state);
void sm_fail(struct sm *m, int fail_state, int rc);
int sm_state(const struct sm *m);

#endif  /* __LIB_SM__ */
