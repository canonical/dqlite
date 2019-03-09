#ifndef DQLITE_DEBUG_H
#define DQLITE_DEBUG_H

#include <libgen.h>
#include <stdio.h>

#include "./lib/logger.h"

#ifdef DQLITE_DEBUG
#define dqlite__debugf(P, FORMAT, ARGS...) \
	if (P->logger != NULL)             \
	debugf(P->logger, FORMAT, ##ARGS)
#else
#define dqlite__debugf(P, FORMAT, ARGS...)
#endif /* DQLITE_DEBUG */

#define dqlite__infof(P, FORMAT, ARGS...) \
	if (P->logger != NULL)            \
	infof(P->logger, FORMAT, ##ARGS)

#define dqlite__errorf(P, FORMAT, ARGS...) \
	if (P->logger != NULL)             \
	errorf(P->logger, FORMAT, ##ARGS)

#endif /* DQLITE_DEBUG_H */
