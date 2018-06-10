#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"
#include "response.h"
#include "message.h"

void dqlite__response_init(struct dqlite__response *r)
{
	assert(r != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_RESPONSE);

	dqlite__error_init(&r->error);
	dqlite__message_init(&r->message);
}

void dqlite__response_close(struct dqlite__response* r)
{
	assert(r != NULL);

	dqlite__message_close(&r->message);
	dqlite__error_close(&r->error);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_RESPONSE);
}

int dqlite__response_welcome(
	struct dqlite__response *r,
	const char* leader,
	uint16_t heartbeat_timeout)
{
	int err;

	assert(r != NULL);
	assert(leader != NULL);
	assert(heartbeat_timeout > 0);

	err = dqlite__message_write_text(&r->message, leader);
	if (err != 0) {
		dqlite__error_wrapf(&r->error, &r->message.error, "failed to encode leader");
		return err;
	}

	err = dqlite__message_write_uint64(&r->message, heartbeat_timeout);
	if (err != 0) {
		dqlite__error_wrapf(&r->error, &r->message.error, "failed to encode heartbeat timeout");
		return err;
	}

	dqlite__message_flush(&r->message, DQLITE_WELCOME, 0);

	return 0;
}

int dqlite__response_servers(struct dqlite__response *r, const char **addresses)
{
	int i;
	int err;

	assert(r != NULL);
	assert(addresses != NULL);

	for(i = 0; *(addresses + i) != NULL; i++) {
		err = dqlite__message_write_text(&r->message, *(addresses + i));
		if (err != 0 ){
			dqlite__error_wrapf(&r->error, &r->message.error, "failed to encode server address %i", i);
			return err;
		}
	}

	dqlite__message_flush(&r->message, DQLITE_SERVERS, 0);

	return 0;
}

int dqlite__response_db(struct dqlite__response *r, uint64_t id)
{
	int err;

	assert(r != NULL);

	err = dqlite__message_write_uint64(&r->message, id);
	if (err != 0) {
		dqlite__error_wrapf(&r->error, &r->message.error, "failed to encode database ID");
		return err;
	}

	dqlite__message_flush(&r->message, DQLITE_DB, 0);

	return 0;
}
