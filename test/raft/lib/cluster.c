#include "cluster.h"

static void randomize(struct raft_fixture *f, unsigned i, int what)
{
    struct raft *raft = raft_fixture_get(f, i);
    switch (what) {
        case RAFT_FIXTURE_TICK:
            /* TODO: provide an API to inspect how much time has elapsed since
             * the last election timer reset */
            if (raft->election_timer_start == raft->io->time(raft->io)) {
                raft_fixture_set_randomized_election_timeout(
                    f, i,
                    munit_rand_int_range(raft->election_timeout,
                                         raft->election_timeout * 2));
            }
            break;
        case RAFT_FIXTURE_DISK:
            raft_fixture_set_disk_latency(f, i, munit_rand_int_range(10, 25));
            break;
        case RAFT_FIXTURE_NETWORK:
            raft_fixture_set_network_latency(f, i,
                                             munit_rand_int_range(25, 50));
            break;
        default:
            munit_assert(0);
            break;
    }
}

void cluster_randomize_init(struct raft_fixture *f)
{
    unsigned i;
    for (i = 0; i < raft_fixture_n(f); i++) {
        randomize(f, i, RAFT_FIXTURE_TICK);
        randomize(f, i, RAFT_FIXTURE_DISK);
        randomize(f, i, RAFT_FIXTURE_NETWORK);
    }
}

void cluster_randomize(struct raft_fixture *f, struct raft_fixture_event *event)
{
    unsigned index = raft_fixture_event_server_index(event);
    int type = raft_fixture_event_type(event);
    randomize(f, index, type);
}
