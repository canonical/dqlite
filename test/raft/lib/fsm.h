/* Test implementation of the raft_fsm interface, with fault injection.
 *
 * The test FSM supports only two commands: setting x and setting y. */

#ifndef TEST_FSM_H
#define TEST_FSM_H

#include "../../../src/raft.h"

void FsmInit(struct raft_fsm *fsm, int version);

/* Same as FsmInit but with asynchronous snapshots */
void FsmInitAsync(struct raft_fsm *fsm, int version);

void FsmClose(struct raft_fsm *fsm);

/* Encode a command to set x to the given value. */
void FsmEncodeSetX(int value, struct raft_buffer *buf);

/* Encode a command to add the given value to x. */
void FsmEncodeAddX(int value, struct raft_buffer *buf);

/* Encode a command to set y to the given value. */
void FsmEncodeSetY(int value, struct raft_buffer *buf);

/* Encode a command to add the given value to y. */
void FsmEncodeAddY(int value, struct raft_buffer *buf);

/* Encode a snapshot of an FSM with the given values for x and y. */
void FsmEncodeSnapshot(int x,
                       int y,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs);

/* Return the current value of x or y. */
int FsmGetX(struct raft_fsm *fsm);
int FsmGetY(struct raft_fsm *fsm);

#endif /* TEST_FSM_H */
