#include "sm.h"
#include <inttypes.h>
#include <stdatomic.h>
#include <stddef.h> /* NULL */
#include <stdio.h> /* fprintf */
#include <string.h>
#include <unistd.h>
#include "../tracing.h"
#include "../utils.h"


static bool sm_is_locked(const struct sm *m)
{
	return ERGO(m->is_locked, m->is_locked(m));
}

int sm_state(const struct sm *m)
{
	PRE(sm_is_locked(m));
	return m->state;
}

static inline void sm_obs(const struct sm *m)
{
	tracef("%s pid: %d sm_id: %" PRIu64 " %s |",
		m->name, m->pid, m->id, m->conf[sm_state(m)].name);
}

void sm_relate(const struct sm *from, const struct sm *to)
{
	tracef("%s-to-%s opid: %d dpid: %d id: %" PRIu64 " id: %" PRIu64 " |",
		from->name, to->name, from->pid, to->pid, from->id, to->id);
}

void sm_attr(const struct sm *m, const char *k, const char *fmt, ...)
{
	char v[SM_MAX_ATTR_LENGTH];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(v, sizeof(v), fmt, ap);
	va_end(ap);
	tracef("%s-attr pid: %d sm_id: %" PRIu64 " %s %s |",
	       m->name, m->pid, m->id, k, v);
}

void sm_init(struct sm *m,
	     bool (*invariant)(const struct sm *, int),
	     bool (*is_locked)(const struct sm *),
	     const struct sm_conf *conf,
		 const char *name,
	     int state)
{
	static atomic_uint_least64_t id = 0;

	PRE(conf[state].flags & SM_INITIAL);

	*m = (struct sm){
		.conf = conf,
		.state = state,
		.invariant = invariant,
		.is_locked = is_locked,
		.id = ++id,
		.pid = getpid(),
		.rc = 0,
	};
	snprintf(m->name, SM_MAX_NAME_LENGTH, "%s", name);
	sm_obs(m);

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
	sm_obs(m);
	POST(m->invariant != NULL && m->invariant(m, prev));
}

void sm_fail(struct sm *m, int fail_state, int rc)
{
	int prev = sm_state(m);
	PRE(sm_is_locked(m));
	PRE(rc != 0 && m->rc == 0);
	PRE(m->conf[fail_state].flags & SM_FAILURE);
	PRE(m->conf[sm_state(m)].allowed & BITS(fail_state));

	m->rc = rc;
	m->state = fail_state;
	sm_obs(m);
	POST(m->invariant != NULL && m->invariant(m, prev));
}

void sm_done(struct sm *m, int good_state, int bad_state, int rc)
{
	int prev = sm_state(m);

	PRE(sm_is_locked(m));
	PRE(m->conf[sm_state(m)].allowed & BITS(good_state));
	PRE(m->conf[sm_state(m)].allowed & BITS(bad_state));
	PRE(m->conf[good_state].flags & SM_FINAL);
	PRE(m->conf[bad_state].flags & SM_FAILURE);

	m->rc = rc;
	m->state = rc == 0 ? good_state : bad_state;
	sm_obs(m);
	POST(m->invariant != NULL && m->invariant(m, prev));
}

static __attribute__((noinline)) bool check_failed(const char *f, int n, const char *s)
{
	tracef("%s:%d check failed: %s", f, n, s);
	return false;
}

bool sm_check(bool b, const char *f, int n, const char *s)
{
	if (!b) {
		return check_failed(f, n, s);
	}
	return true;
}
