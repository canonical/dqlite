#ifndef LIFECYCLE_H_
#define LIFECYCLE_H_

#include "../raft.h"
#include "request.h"

void lifecycleRequestStart(struct raft *r, struct request *req);
void lifecycleRequestEnd(struct raft *r, struct request *req);

#endif
