#include <assert.h>
#include <stdint.h>
#include <string.h>

#include "dqlite.h"
#include "error.h"
#include "lifecycle.h"
#include "message.h"
#include "request.h"

/* Set the maximum data size to 1M, to prevent eccessive memmory allocation
 * in case of client bugs
 *
 * TODO: relax this limit since it prevents inserting large blobs.
 */
#define DQLITE_REQUEST_MAX_DATA_SIZE 1048576

static const char *dqlite__request_type_names[] = {
	"Helo",      /* DQLITE_HELO */
	"Heartbeat", /* DQLITE_HEARTBEAT */
	"Open",      /* DQLITE_OPEN */
	0
};

void dqlite__request_init(struct dqlite__request *r)
{
	assert(r != NULL);

	dqlite__lifecycle_init(DQLITE__LIFECYCLE_REQUEST);

	r->timestamp = 0;

	dqlite__error_init(&r->error);
	dqlite__message_init(&r->message);
}

void dqlite__request_close(struct dqlite__request* r)
{
	assert(r != NULL);

	dqlite__error_close(&r->error);
	dqlite__message_close(&r->message);

	dqlite__lifecycle_close(DQLITE__LIFECYCLE_REQUEST);
}

int dqlite__request_header_received(struct dqlite__request *r)
{
	int err;
	assert(r != NULL);

	err = dqlite__message_header_received(&r->message);
	if (err != 0) {
		dqlite__error_wrapf(&r->error, &r->message.error, "failed to parse request header");
	}

	return 0;
}

/* At the moment we're setting a cap to the request size. Also, for requests
 * with a fixed message body length, check that the length of the received
 * message body actually matches the expected one. */
static int dqlite__request_body_len_check(struct dqlite__request *r)
{
	uint16_t type;
	size_t len;
	size_t expected_len;

	assert(r != NULL);

	type = dqlite__request_type(r);
	len = dqlite__message_body_len(&r->message);

	if (len > DQLITE_REQUEST_MAX_DATA_SIZE) {
		dqlite__error_printf(&r->error, "request too large: %ld", len);
		return DQLITE_PARSE;
	}

	switch (type) {

	case DQLITE_HELO:
		expected_len = DQLITE__MESSAGE_WORD_SIZE;
		break;

	case DQLITE_HEARTBEAT:
		expected_len = DQLITE__MESSAGE_WORD_SIZE;
		break;

	default:
		return 0;
	}

	if (len != expected_len) {
		const char *name = dqlite__request_type_name(r);
		dqlite__error_printf(&r->error, "malformed %s: %ld bytes", name, len);
		return DQLITE_PARSE;
	}

	return 0;
}

static int dqlite__request_helo_received(struct dqlite__request *r)
{
	int err;

	assert(r != NULL);

	err = dqlite__message_read_uint64(&r->message, &r->helo.client_id);
	if (err != 0 && err != DQLITE_EOM) {
		dqlite__error_wrapf(&r->error, &r->message.error, "failed to parse client ID");
		return err;
	}

	return err;
}

static int dqlite__request_heartbeat_received(struct dqlite__request *r)
{
	int err;

	assert(r != NULL);

	err = dqlite__message_read_uint64(&r->message, &r->heartbeat.timestamp);
	if (err != 0 && err != DQLITE_EOM) {
		dqlite__error_wrapf(&r->error, &r->message.error, "failed to parse timestamp");
		return err;
	}

	return err;
}

static int dqlite__request_open_received(struct dqlite__request *r)
{
	int err;

	assert(r != NULL);

	err = dqlite__message_read_text(&r->message, &r->open.name);
	if (err != 0 && err != DQLITE_EOM) {
		dqlite__error_wrapf(&r->error, &r->message.error, "failed to parse name");
		return err;
	}

	return err;
}

int dqlite__request_body_received(struct dqlite__request *r)
{
	int err;
	uint16_t type;

	assert(r != NULL);

	err = dqlite__request_body_len_check(r);
	if (err != 0) {
		return err;
	}

	type = dqlite__request_type(r);

	switch (type) {

	case DQLITE_HELO:
		err = dqlite__request_helo_received(r);
		break;

	case DQLITE_HEARTBEAT:
		err = dqlite__request_heartbeat_received(r);
		break;

	case DQLITE_OPEN:
		err = dqlite__request_open_received(r);
		break;

	default:
		dqlite__error_printf(&r->error, "unknown request type");
		err = DQLITE_PROTO;
		break;
	}

	if (err != DQLITE_EOM) {
		const char *name = dqlite__request_type_name(r);
		dqlite__error_wrapf(&r->error, &r->error, "failed to parse %s", name);
		return err;
	}

	return 0;
}

void dqlite__request_processed(struct dqlite__request *r)
{
	dqlite__message_processed(&r->message);
}

uint16_t dqlite__request_type(struct dqlite__request *r){
	assert(r != NULL);

	return r->message.type;
}

const char *dqlite__request_type_name(struct dqlite__request *r)
{
	int type;
	int i;

	assert(r != NULL);

	type = dqlite__request_type(r);

	for (i = 0; dqlite__request_type_names[i] != NULL; i++) {
		if (i == type) {
			return dqlite__request_type_names[type];
		}
	}

	return "Unknown";
}
