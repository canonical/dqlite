#ifndef DQLITE_DEBUG_H
#define DQLITE_DEBUG_H

#include <stdio.h>
#include <libgen.h>

#define dqlite__errorf(P, MSG, CTX, ARGS...)				\
	fprintf(P->log, "ERRO: %10s (%40s:%4d): %-25s " CTX "\n", basename(__FILE__), __func__, __LINE__, MSG, ##ARGS)

#define dqlite__infof(P, MSG, CTX, ARGS...)				\
	fprintf(P->log, "INFO: %10s (%40s:%4d): %-25s " CTX "\n", basename(__FILE__), __func__, __LINE__, MSG, ##ARGS)

#ifdef DQLITE_DEBUG
#define dqlite__debugf(P, MSG, CTX, ARGS...)				\
	fprintf(P->log, "DEBG: %10s (%40s:%4d): %-25s " CTX "\n", basename(__FILE__), __func__, __LINE__, MSG, ##ARGS)
#else
#define dqlite__debugf(D, FORMAT, ARG...)
#endif /* DQLITE_DEBUG */

#endif /* DQLITE_DEBUG_H */
