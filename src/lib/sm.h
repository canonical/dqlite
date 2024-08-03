#ifndef __LIB_SM__
#define __LIB_SM__

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>

#define BITS(state) (1ULL << (state))

#define CHECK(cond) sm_check((cond), __FILE__, __LINE__, #cond)

#define SM_MAX_NAME_LENGTH 50
#define SM_MAX_ATTR_LENGTH 100

enum {
	SM_PREV_NONE = -1,
	/* sizeof(sm_conf::allowed * 8) */
	SM_STATES_MAX = 64,
	/* flags */
	SM_INITIAL = 1U << 0,
	SM_FAILURE = 1U << 1,
	SM_FINAL = 1U << 2,
};

struct sm_conf
{
	uint32_t flags;
	uint64_t allowed;
	const char *name;
};

struct sm
{
	int rc;
	int state;
	char name[SM_MAX_NAME_LENGTH];
	uint64_t id;
	pid_t pid;
	bool (*is_locked)(const struct sm *);
	bool (*invariant)(const struct sm *, int);
	const struct sm_conf *conf;
};

void sm_init(struct sm *m,
	     bool (*invariant)(const struct sm *, int),
	     /* optional, set NULL if not used */
	     bool (*is_locked)(const struct sm *),
	     const struct sm_conf *conf,
		 const char *name,
	     int state);
void sm_fini(struct sm *m);
void sm_move(struct sm *m, int next_state);
void sm_fail(struct sm *m, int fail_state, int rc);
int sm_state(const struct sm *m);
bool sm_check(bool b, const char *f, int n, const char *s);
/* Relates one state machine to another for observability. */
void sm_relate(const struct sm *from, const struct sm *to);
/**
 * Records an attribute of a state machine for observability.
 */
void sm_attr(const struct sm *m, const char *k, const char *fmt, ...);

#endif /* __LIB_SM__ */
