#include "../../src/protocol.h"
#include "../../src/roles.h"
#include "../../src/tracing.h"
#include "../lib/runner.h"

/* DSL for writing declarative role-management tests */

#define VOTERS(x) x
#define STANDBYS(x) x
#define ONLINE true
#define OFFLINE false
#define FAILURE_DOMAIN(x) x
#define WEIGHT(x) x

#define TARGET(voters_, standbys_)               \
	do {                                     \
		struct adjust_fixture *f = data; \
		f->voters = voters_;             \
		f->standbys = standbys_;         \
		f->n = 0;                        \
	} while (0)

#define BEFORE(id_, role_, online_, failure_domain_, weight_)    \
	do {                                                     \
		struct adjust_fixture *f = data;                 \
		munit_assert_uint(id_, >, 0);                    \
		munit_assert_uint(id_, <=, 10);                  \
		munit_assert_uint(id_, ==, f->n + 1);            \
		f->nodes[f->n].id = id_;                         \
		f->nodes[f->n].role = role_;                     \
		f->nodes[f->n].online = online_;                 \
		f->nodes[f->n].failure_domain = failure_domain_; \
		f->nodes[f->n].weight = weight_;                 \
		f->n += 1;                                       \
	} while (0)

#define COMPUTE(id_)                                                        \
	do {                                                                \
		struct adjust_fixture *f = data;                            \
		RolesComputeChanges(f->voters, f->standbys, f->nodes, f->n, \
				    id_, applyChangeCb, f);                 \
	} while (0)

#define AFTER(id_, role_)                                               \
	do {                                                            \
		unsigned i_;                                            \
		struct adjust_fixture *f = data;                        \
		munit_assert_uint(id_, >, 0);                           \
		munit_assert_uint(id_, <=, f->n);                       \
		for (i_ = 0; i_ < f->n; i_ += 1) {                      \
			if (f->nodes[i_].id == id_) {                   \
				munit_assert_int(f->nodes[i_].role, ==, \
						 role_);                \
				break;                                  \
			}                                               \
		}                                                       \
		if (i_ == f->n) {                                       \
			munit_assert(false);                            \
		}                                                       \
	} while (0)

TEST_MODULE(role_management);

TEST_SUITE(adjust);

struct adjust_fixture
{
	int voters;
	int standbys;
	unsigned n;
	struct all_node_info nodes[10];
};

static void applyChangeCb(uint64_t id, int role, void *arg)
{
	(void)id;
	(void)role;
	(void)arg;
}

TEST_SETUP(adjust)
{
	(void)params;
	(void)user_data;
	struct adjust_fixture *f = munit_malloc(sizeof *f);
	memset(f, 0, sizeof *f);
	return f;
}

TEST_TEAR_DOWN(adjust)
{
	free(data);
}

/* A standby is promoted when there aren't enough voters. */
TEST_CASE(adjust, promote_voter, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(0));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_VOTER);
	return MUNIT_OK;
}

/* A voter is demoted when there are too many voters. */
TEST_CASE(adjust, demote_voter, NULL)
{
	(void)params;
	TARGET(VOTERS(1), STANDBYS(0));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_SPARE);
	return MUNIT_OK;
}

/* A spare is promoted when there aren't enough standbys. */
TEST_CASE(adjust, promote_standby, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(4, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_VOTER);
	AFTER(4, DQLITE_STANDBY);
	return MUNIT_OK;
}

/* A standby is demoted when there are too many standbys. */
TEST_CASE(adjust, demote_standby, NULL)
{
	(void)params;
	TARGET(VOTERS(1), STANDBYS(0));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_SPARE);
	return MUNIT_OK;
}

/* An offline node is demoted, even when there's a shortage of voters and
 * standbys. */
TEST_CASE(adjust, demote_offline, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_STANDBY, OFFLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_SPARE);
	return MUNIT_OK;
}

/* An offline voter is demoted and an online spare is promoted. */
TEST_CASE(adjust, voter_online_exchange, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(0));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_VOTER, OFFLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(4, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_SPARE);
	AFTER(4, DQLITE_VOTER);
	return MUNIT_OK;
}

/* An offline standby is demoted and an online spare is promoted. */
TEST_CASE(adjust, standby_online_exchange, NULL)
{
	(void)params;
	TARGET(VOTERS(1), STANDBYS(1));
	BEFORE(1, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_STANDBY, OFFLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(3);
	AFTER(1, DQLITE_STANDBY);
	AFTER(2, DQLITE_SPARE);
	AFTER(3, DQLITE_VOTER);
	return MUNIT_OK;
}

/* A standby is promoted to voter, and a spare replaces it. */
TEST_CASE(adjust, voter_standby_promote_succession, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(1));
	BEFORE(1, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(4, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(4);
	AFTER(1, DQLITE_STANDBY);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_VOTER);
	AFTER(4, DQLITE_VOTER);
	return MUNIT_OK;
}

/* A standby with a distinctive failure domain is preferred for promotion. */
TEST_CASE(adjust, voter_failure_domains, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(4, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(2), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_STANDBY);
	AFTER(4, DQLITE_VOTER);
	return MUNIT_OK;
}

/* A spare with a distinctive failure domain is preferred for promotion. */
TEST_CASE(adjust, standby_failure_domains, NULL)
{
	(void)params;
	TARGET(VOTERS(1), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(2), WEIGHT(1));
	BEFORE(3, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_STANDBY);
	AFTER(3, DQLITE_SPARE);
	return MUNIT_OK;
}

/* An offline standby is demoted even when it has a distinctive failure domain.
 */
TEST_CASE(adjust, voter_failure_domains_vs_online, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(4, DQLITE_STANDBY, OFFLINE, FAILURE_DOMAIN(2), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_VOTER);
	AFTER(4, DQLITE_SPARE);
	return MUNIT_OK;
}

/* An offline spare is not promoted even when it has a distinctive failure
 * domain. */
TEST_CASE(adjust, standby_failure_domains_vs_online, NULL)
{
	(void)params;
	TARGET(VOTERS(1), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_SPARE, OFFLINE, FAILURE_DOMAIN(2), WEIGHT(1));
	BEFORE(3, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_SPARE);
	AFTER(3, DQLITE_STANDBY);
	return MUNIT_OK;
}

/* A standby with a lower weight is preferred for promotion. */
TEST_CASE(adjust, voter_weights, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(2));
	BEFORE(4, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_STANDBY);
	AFTER(4, DQLITE_VOTER);
	return MUNIT_OK;
}

/* A spare with a lower weight is preferred for promotion. */
TEST_CASE(adjust, standby_weights, NULL)
{
	(void)params;
	TARGET(VOTERS(1), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(2));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_STANDBY);
	AFTER(3, DQLITE_SPARE);
	return MUNIT_OK;
}

/* A standby with a distinctive failure domain is preferred for promotion over
 * one with a low weight. */
TEST_CASE(adjust, voter_weights_vs_failure_domains, NULL)
{
	(void)params;
	TARGET(VOTERS(3), STANDBYS(1));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(2), WEIGHT(2));
	BEFORE(4, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_VOTER);
	AFTER(3, DQLITE_VOTER);
	AFTER(4, DQLITE_STANDBY);
	return MUNIT_OK;
}

/* A spare with a distinctive failure domain is preferred for promotion over one
 * with a low weight. */
TEST_CASE(adjust, standby_weights_vs_failure_domains, NULL)
{
	(void)params;
	TARGET(VOTERS(1), STANDBYS(2));
	BEFORE(1, DQLITE_VOTER, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(2, DQLITE_STANDBY, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(3, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(1), WEIGHT(1));
	BEFORE(4, DQLITE_SPARE, ONLINE, FAILURE_DOMAIN(2), WEIGHT(2));
	COMPUTE(1);
	AFTER(1, DQLITE_VOTER);
	AFTER(2, DQLITE_STANDBY);
	AFTER(3, DQLITE_SPARE);
	AFTER(4, DQLITE_STANDBY);
	return MUNIT_OK;
}
