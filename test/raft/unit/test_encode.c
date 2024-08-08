#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../lib/runner.h"
#include "../../../src/raft.h"
#include "../../../src/raft/byte.h"
#include "../../../src/raft/uv_encoding.h"

SUITE(encode)

static void *set_up(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
	return NULL;
}

static void tear_down(void *data)
{
	(void)data;
}

/* This macro is used together with END to create necessary auxiliary structs
 * and to serialize and deserialize a message. It expects a raft_message called
 * `msg` to be defined and it will define another one called `decoded` based on
 * serializing and deserializing `msg`. Lastly, END frees the memory of the
 * auxiliary buffers. Usage:
 *     START;
 *     <my assertions using msg and decoded>
 *     END;
 */
#define START \
	struct raft_message decoded; \
	const struct raft_message *_msg = (&msg); \
 \
	uv_buf_t *_bufs; \
	unsigned _n_bufs; \
	uvEncodeMessage(_msg, &_bufs, &_n_bufs); \
	munit_assert_int(_n_bufs, ==, 1); \
 \
	/* Consume the fields in the header that uvDecodeMessage does not expect.*/ \
	uv_buf_t _cursor = *_bufs; \
	munit_assert_int(byteGet64((void *)&_cursor.base), ==, _msg->type); \
	byteGet64((void *)&_cursor.base); \
	size_t _payload_len; \
	uvDecodeMessage(_msg->type, &_cursor, &decoded, &_payload_len);

#define END \
	free(_bufs[0].base); \
	free(_bufs); \
 \
	return MUNIT_OK; \


TEST(encode, signature, set_up, tear_down, 0, NULL) {
	const struct raft_message msg = {
		.type = RAFT_IO_SIGNATURE,
		.signature = (struct raft_signature) {
			.version = 0,
			.db = "test-db",
			.ask_calculated = true,
			.result = RAFT_RESULT_DONE,
			.page_from_to = (struct page_from_to){.from = 37, .to = 1337},
		}
	};

	START;

	const struct raft_signature *m1 = &msg.signature;
	const struct raft_signature *m2 = &decoded.signature;
	munit_assert_int(m1->version, ==, m2->version);
	munit_assert_int(m1->ask_calculated, ==, m2->ask_calculated);
	munit_assert_int(m1->page_from_to.from, ==, m2->page_from_to.from);
	munit_assert_int(m1->page_from_to.to, ==, m2->page_from_to.to);
	munit_assert_int(m1->result, ==, m2->result);
	munit_assert_string_equal(m1->db, m2->db);

	END;
}

TEST(encode, signature_result, set_up, tear_down, 0, NULL) {
	const struct raft_message msg = {
		.type = RAFT_IO_SIGNATURE_RESULT,
		.signature_result = (struct raft_signature_result) {
			.version = 0,
			.db = "test-db",
			.result = RAFT_RESULT_DONE,
			.cs_nr = 3,
			.cs = {
				(struct page_checksum){.checksum = 1234, .page_no = 37},
				(struct page_checksum){.checksum = 1723848, .page_no = 1},
				(struct page_checksum){.checksum = 93482, .page_no = 23498},
			}
		}
	};

	START;

	const struct raft_signature_result *m1 = &msg.signature_result;
	const struct raft_signature_result *m2 = &decoded.signature_result;
	munit_assert_int(m1->version, ==, m2->version);
	munit_assert_int(m1->calculated, ==, m2->calculated);
	munit_assert_int(m1->cs_nr, ==, m2->cs_nr);
	munit_assert_int(m1->result, ==, m2->result);
	for (size_t i = 0; i < msg.signature_result.cs_nr; i++) {
		munit_assert_int(m1->cs[i].checksum, ==, m2->cs[i].checksum);
		munit_assert_int(m1->cs[i].page_no, ==, m2->cs[i].page_no);
	}
	munit_assert_string_equal(m1->db, m2->db);

	END;
}

TEST(encode, install_snapshot_result, set_up, tear_down, 0, NULL) {
	const struct raft_message msg = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_RESULT,
		.install_snapshot_result = (struct raft_install_snapshot_result) {
			.version = 0,
			.result = RAFT_RESULT_DONE,
		}
	};

	START;

	const struct raft_install_snapshot_result *m1 =
		&msg.install_snapshot_result;
	const struct raft_install_snapshot_result *m2 =
		&decoded.install_snapshot_result;
	munit_assert_int(m1->version, ==, m2->version);
	munit_assert_int(m1->result, ==, m2->result);

	END;
}

TEST(encode, install_snapshot_cp, set_up, tear_down, 0, NULL) {
	const uint8_t *page_data[4096];
	for (size_t i = 0; i < 4096; i++) {
		/* TODO: should this be in a separate raft_buffer like APPEND_ENTRIES
		 * right now? */
		((uint8_t *)page_data)[i] = i;
	}
	const struct raft_message msg = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_CP,
		.install_snapshot_cp = (struct raft_install_snapshot_cp) {
			.version = 0,
			.db = "test-db",
			.page_no = 92839,
			.page_data = (struct raft_buffer) {.base = page_data, .len = 4096},
			.result = RAFT_RESULT_DONE,
		}
	};

	START;

	const struct raft_install_snapshot_cp *m1 =
		&msg.install_snapshot_cp;
	const struct raft_install_snapshot_cp *m2 =
		&decoded.install_snapshot_cp;
	munit_assert_int(m1->version, ==, m2->version);
	munit_assert_int(m1->page_no, ==, m2->page_no);
	munit_assert_int(m1->result, ==, m2->result);
	munit_assert_int(m1->page_data.len, ==, m2->page_data.len);
	for (size_t i = 0; i < m1->page_data.len; i++) {
		munit_assert_int(
				((uint8_t *)m1->page_data.base)[i],
				==,
				((uint8_t *)m2->page_data.base)[i]);
	}
	munit_assert_string_equal(m1->db, m2->db);

	END;
}

TEST(encode, install_snapshot_cp_result, set_up, tear_down, 0, NULL) {
	const struct raft_message msg = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT,
		.install_snapshot_cp_result = (struct raft_install_snapshot_cp_result) {
			.version = 0,
			.last_known_page_no = 57,
			.result = RAFT_RESULT_DONE,
		}
	};

	START;

	const struct raft_install_snapshot_cp_result *m1 =
		&msg.install_snapshot_cp_result;
	const struct raft_install_snapshot_cp_result *m2 =
		&decoded.install_snapshot_cp_result;
	munit_assert_int(m1->version, ==, m2->version);
	munit_assert_int(m1->last_known_page_no, ==, m2->last_known_page_no);
	munit_assert_int(m1->result, ==, m2->result);

	END;
}

TEST(encode, install_snapshot_mv, set_up, tear_down, 0, NULL) {
	struct raft_message msg = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_MV,
		.install_snapshot_mv = (struct raft_install_snapshot_mv) {
			.version = 0,
			.result = RAFT_RESULT_DONE,
			.mv_nr = 8,
			.db = "test-db",
			.mv = {},
		}
	};
	for (size_t i = 0; i < 8; i++) {
		msg.install_snapshot_mv.mv[i].from = (i+1) * 13;
		msg.install_snapshot_mv.mv[i].to = (i+1) * 7;
	}

	START;

	const struct raft_install_snapshot_mv *m1 =
		&msg.install_snapshot_mv;
	const struct raft_install_snapshot_mv *m2 =
		&decoded.install_snapshot_mv;
	munit_assert_int(m1->version, ==, m2->version);
	munit_assert_int(m1->mv_nr, ==, m2->mv_nr);
	munit_assert_int(m1->result, ==, m2->result);
	for (size_t i = 0; i < m1->mv_nr; i++) {
		munit_assert_int(
				m1->mv[i].from,
				==,
				m2->mv[i].from);
		munit_assert_int(
				m1->mv[i].to,
				==,
				m2->mv[i].to);
	}
	munit_assert_string_equal(m1->db, m2->db);

	END;
}

TEST(encode, install_snapshot_mv_result, set_up, tear_down, 0, NULL) {
	const struct raft_message msg = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT,
		.install_snapshot_mv_result = (struct raft_install_snapshot_mv_result) {
			.version = 0,
			.last_known_page_no = 57,
			.result = RAFT_RESULT_DONE,
			.db = "test-db",
		}
	};

	START;

	const struct raft_install_snapshot_mv_result *m1 =
		&msg.install_snapshot_mv_result;
	const struct raft_install_snapshot_mv_result *m2 =
		&decoded.install_snapshot_mv_result;
	munit_assert_int(m1->version, ==, m2->version);
	munit_assert_int(m1->last_known_page_no, ==, m2->last_known_page_no);
	munit_assert_int(m1->result, ==, m2->result);
	munit_assert_string_equal(m1->db, m2->db);

	END;
}
