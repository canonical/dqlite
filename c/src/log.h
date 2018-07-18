#ifndef DQLITE_DEBUG_H
#define DQLITE_DEBUG_H

#include <libgen.h>
#include <stdio.h>

#include "../include/dqlite.h"

#ifdef DQLITE_DEBUG
#define dqlite__debugf(P, FORMAT, ARGS...)                                          \
	if (P->logger != NULL)                                                      \
	P->logger->xLogf(P->logger->ctx, DQLITE_LOG_DEBUG, FORMAT, ##ARGS)
#else
#define dqlite__debugf(P, FORMAT, ARGS...)
#endif /* DQLITE_DEBUG */

#define dqlite__infof(P, FORMAT, ARGS...)                                           \
	if (P->logger != NULL)                                                      \
	P->logger->xLogf(P->logger->ctx, DQLITE_LOG_INFO, FORMAT, ##ARGS)

#define dqlite__errorf(P, FORMAT, ARGS...)                                          \
	if (P->logger != NULL)                                                      \
	P->logger->xLogf(P->logger->ctx, DQLITE_LOG_ERROR, FORMAT, ##ARGS)

#endif /* DQLITE_DEBUG_H */
