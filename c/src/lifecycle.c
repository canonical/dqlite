#include <stdio.h>
#include <string.h>
#include <libgen.h>

#include "lifecycle.h"

#ifdef DQLITE_DEBUG

/* End marker for the dqlite__lifecycle_refcount array */
#define DQLITE__LIFECYCLE_REFCOUNT_NULL -(1 << 24)

static char *dqlite__lifecycle_type_names[] = {
	"dqlite__error",      /* DQLITE__LIFECYCLE_ERROR */
	"dqlite__fsm",        /* DQLITE__LIFECYCLE_FSM */
	"dqlite__message",    /* DQLITE__LIFECYCLE_MESSAGE */
	"dqlite__request",    /* DQLITE__LIFECYCLE_REQUEST */
	"dqlite__response",   /* DQLITE__LIFECYCLE_RESPONSE */
	"dqlite__gateway",    /* DQLITE__LIFECYCLE_GATEWAY */
	"dqlite__conn",       /* DQLITE__LIFECYCLE_CONN */
	"dqlite__queue",      /* DQLITE__LIFECYCLE_QUEUE */
	"dqlite__queue_item", /* DQLITE__LIFECYCLE_QUEUE_ITEM */
	"dqlite__db",         /* DQLITE__LIFECYCLE_DB */
};

static int dqlite__lifecycle_refcount[] = {
	0, /* DQLITE__LIFECYCLE_ERROR */
	0, /* DQLITE__LIFECYCLE_FSM */
	0, /* DQLITE__LIFECYCLE_MESSAGE */
	0, /* DQLITE__LIFECYCLE_REQUEST */
	0, /* DQLITE__LIFECYCLE_RESPONSE */
	0, /* DQLITE__LIFECYCLE_GATEWAY */
	0, /* DQLITE__LIFECYCLE_CONN */
	0, /* DQLITE__LIFECYCLE_QUEUE */
	0, /* DQLITE__LIFECYCLE_QUEUE_ITEM */
	0, /* DQLITE__LIFECYCLE_DB */
	DQLITE__LIFECYCLE_REFCOUNT_NULL
};

static char dqlite__lifecycle_errmsg[4096];

void dqlite__lifecycle_init(int type) {
	dqlite__lifecycle_refcount[type]++;
}

void dqlite__lifecycle_close(int type) {
	dqlite__lifecycle_refcount[type]--;
}

#define DQLITE__LIFECYCLE_ERRMSG \
	sprintf(dqlite__lifecycle_errmsg, "%s%s:%d: %s lifecycle leak: %d\n", \
		dqlite__lifecycle_errmsg, \
		basename(__FILE__),	  \
		__LINE__, name, refcount)

static void dqlite__lifecycle_check_type(int type)
{
	int refcount = dqlite__lifecycle_refcount[type];
	char *name = dqlite__lifecycle_type_names[type];

	if (refcount == 0) {
		return;
	}

	/* Reset the counter for the next time dqlite__lifecycle_check gets
	 * called */
	dqlite__lifecycle_refcount[type] = 0;

	DQLITE__LIFECYCLE_ERRMSG;
}

int dqlite__lifecycle_check(char **errmsg)
{
	int type;

	dqlite__lifecycle_errmsg[0] = 0;

	for (type = 0; dqlite__lifecycle_refcount[type] != DQLITE__LIFECYCLE_REFCOUNT_NULL; type++) {
		dqlite__lifecycle_check_type(type);
	}

	if (strlen(dqlite__lifecycle_errmsg) > 0) {
		*errmsg = dqlite__lifecycle_errmsg;
		return -1;
	}

	*errmsg = 0;

	return 0;
}

#endif /* DQLITE_DEBUG */
