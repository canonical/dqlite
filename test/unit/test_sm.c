#include <sqlite3.h>

#include "../../src/lib/sm.h"

#include "../lib/runner.h"

TEST_MODULE(sm);

/******************************************************************************
 *
 * SM.
 *
 ******************************************************************************/

TEST_SUITE(sm);

/**
 * An example of simple state machine.
 *
 *           TRANSIENT
 *              | ^
 *    restarted | | crashed
 *              V |
 *            ONLINE--------+ checked
 *               |  <-------+
 *       stopped |
 *               V
 *	      OFFLINE
 */

enum states {
	S_ONLINE,
	S_OFFLINE,
	S_TRANSIENT,
	S_NR,
};

static const struct sm_conf op_states[S_NR] = {
    [S_ONLINE] =
	{
	    .flags = SM_INITIAL,
	    .name = "online",
	    .allowed = BITS(S_ONLINE) | BITS(S_TRANSIENT) | BITS(S_OFFLINE),
	},
    [S_TRANSIENT] = {.flags = SM_FAILURE,
		     .name = "transient",
		     .allowed = BITS(S_ONLINE)},
    [S_OFFLINE] =
	{
	    .flags = SM_FINAL,
	    .name = "offline",
	    .allowed = 0,
	},
};

enum triggers {
	T_RESTARTED,
	T_CRASHED,
	T_CHECKED,
	T_STOPPED,
};

struct op_states_sm
{
	struct sm sm;
	enum triggers sm_trigger;
};

static bool sm_invariant(const struct sm *m, int prev_state)
{
	struct op_states_sm *sm = CONTAINER_OF(m, struct op_states_sm, sm);

	return ERGO(sm_state(m) == S_ONLINE,
		    ERGO(prev_state == SM_PREV_NONE, sm->sm_trigger == 0)) &&
	       ERGO(sm_state(m) == S_ONLINE,
		    ERGO(prev_state == S_ONLINE,
			 sm->sm_trigger == BITS(T_CHECKED)) &&
			ERGO(prev_state == S_TRANSIENT,
			     sm->sm_trigger == BITS(T_RESTARTED))) &&
	       ERGO(sm_state(m) == S_TRANSIENT,
		    sm->sm_trigger == BITS(T_CRASHED) && m->rc == -42) &&
	       ERGO(sm_state(m) == S_OFFLINE,
		    sm->sm_trigger == BITS(T_STOPPED));
}

TEST_CASE(sm, simple, NULL)
{
	(void)data;
	(void)params;
	struct op_states_sm sm = {};
	struct sm *m = &sm.sm;

	sm_init(&sm.sm, sm_invariant, NULL, op_states, "test", S_ONLINE);

	sm.sm_trigger = BITS(T_CHECKED);
	sm_move(m, S_ONLINE);
	sm_move(m, S_ONLINE);
	sm_move(m, S_ONLINE);

	sm.sm_trigger = BITS(T_CRASHED);
	sm_fail(m, S_TRANSIENT, -42 /* -rc */);

	sm.sm_trigger = BITS(T_RESTARTED);
	sm_move(m, S_ONLINE);

	sm.sm_trigger = BITS(T_STOPPED);
	sm_move(m, S_OFFLINE);

	sm_fini(m);
	return 0;
}
