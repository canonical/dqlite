#include "src/lib/sm.h"
#include "src/utils.h"
#include <stddef.h> /* NULL */


static bool sm_is_locked(const struct sm *)
{
	return true; /* dummy so far */
}

int sm_state(const struct sm *m)
{
	PRE(sm_is_locked(m));
	return m->state;
}

void sm_init(struct sm *m, bool (*invariant)(const struct sm *, int),
	     const struct sm_conf *conf, int state)
{
	PRE(conf[state].flags & SM_INITIAL);

	m->conf = conf;
	m->state = state;
	m->invariant = invariant;

	POST(m->invariant != NULL && m->invariant(m, SM_PREV_NONE));
}

void sm_fini(struct sm *m)
{
	PRE(m->invariant != NULL && m->invariant(m, SM_PREV_NONE));
	PRE(m->conf[sm_state(m)].flags & SM_FINAL);
}

void sm_move(struct sm *m, int next_state)
{
	int prev = sm_state(m);

	PRE(sm_is_locked(m));
	PRE(m->conf[sm_state(m)].allowed & BITS(next_state));
	m->state = next_state;
	POST(m->invariant != NULL && m->invariant(m, prev));
}

void sm_fail(struct sm *m, int fail_state, int rc)
{
	int prev = sm_state(m);
	PRE(sm_is_locked(m));
	PRE(rc != 0 && m->rc == 0);
	PRE(m->conf[sm_state(m)].flags & SM_FAILURE);
	PRE(m->conf[sm_state(m)].allowed & BITS(fail_state));

	m->rc = rc;
	m->state = fail_state;
	POST(m->invariant != NULL && m->invariant(m, prev));
}
