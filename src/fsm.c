#include <assert.h>

#include <sqlite3.h>

#include "../include/dqlite.h"

#include "fsm.h"
#include "lifecycle.h"

void dqlite__fsm_init(struct dqlite__fsm *            f,
                      struct dqlite__fsm_state *      states,
                      struct dqlite__fsm_event *      events,
                      struct dqlite__fsm_transition **transitions) {
	int                             i;
	int                             j;
	struct dqlite__fsm_transition **cursor;

	assert(f != NULL);

	assert(states != NULL);
	assert(events != NULL);
	assert(transitions != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_FSM);

	f->events      = events;
	f->states      = states;
	f->transitions = transitions;

	f->curr_state_id = 0;
	f->next_state_id = 0;
	f->jump_state_id = -1;

	/* Count the number of valid events */
	f->events_count = 0;
	for (i = 0; events[i].id != DQLITE__FSM_NULL; i++) {
		assert(events[i].id == i);
		f->events_count++;
	}

	/* Count the number of valid states */
	f->states_count = 0;
	for (i = 0; states[i].id != DQLITE__FSM_NULL; i++) {
		assert(states[i].id == i);
		f->states_count++;
	}

	/* Verify the transitions index */
	for (i = 0; i < f->states_count; i++) {
		cursor = &f->transitions[i];

		for (j = 0; j < f->events_count; j++) {
			assert(j == (*cursor)[j].event_id);
		}
	}
}

void dqlite__fsm_close(struct dqlite__fsm *f) {
	assert(f != NULL);

	/* No-op */

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_FSM);
}

int dqlite__fsm_step(struct dqlite__fsm *f, int event_id, void *arg) {
	int                            err;
	struct dqlite__fsm_transition *transition;

	assert(f != NULL);
	assert(arg != NULL);
	assert(event_id < f->events_count);

	transition = &f->transitions[f->curr_state_id][event_id];

	assert(transition != NULL);
	assert(transition->next_state_id < f->states_count);

	assert(transition->callback != NULL);

	err = (*transition->callback)(arg);
	if (err != 0)
		return err;

	/* Handlers can set jump_state_id if they want to skip the normal FSM
	 * flow and jump to a different state. */
	if (f->jump_state_id != -1) {
		assert(-f->jump_state_id < f->states_count);
		f->next_state_id = f->jump_state_id;
		f->jump_state_id = -1;
	} else {
		f->next_state_id = transition->next_state_id;
	}

	f->curr_state_id = f->next_state_id;

	return 0;
}

const char *dqlite__fsm_state(struct dqlite__fsm *f) {
	assert(f != NULL);
	assert(f->curr_state_id < f->states_count);

	return f->states[f->curr_state_id].name;
}
