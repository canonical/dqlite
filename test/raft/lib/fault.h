/* Helper for test components supporting fault injection. */

#ifndef TEST_FAULT_H
#define TEST_FAULT_H

#include <stdbool.h>

/* Information about a fault that should occur in a component. */
struct Fault
{
    int countdown; /* Trigger the fault when this counter gets to zero. */
    int n;         /* Repeat the fault this many times. Default is -1. */
    bool paused;   /* Pause fault triggering. */
};

/* Initialize a fault. */
void FaultInit(struct Fault *f);

/* Advance the counters of the fault. Return true if the fault should be
 * triggered, false otherwise. */
bool FaultTick(struct Fault *f);

/* Configure the fault with the given values. */
void FaultConfig(struct Fault *f, int delay, int repeat);

/* Pause triggering configured faults. */
void FaultPause(struct Fault *f);

/* Resume triggering configured faults. */
void FaultResume(struct Fault *f);

#endif /* TESTFAULT_H */
