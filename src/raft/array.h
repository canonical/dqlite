/* Macros to manipulate contiguous arrays. */

#ifndef ARRAY_H_
#define ARRAY_H_

#include "../raft.h"

/* Append item I of type T to array A which currently has N items.
 *
 * A and N must both by pointers. Set RV to -1 in case of failure. */
#define ARRAY__APPEND(T, I, A, N, RV)                                \
	{                                                            \
		T *tmp_array;                                        \
		tmp_array = raft_realloc(*A, (*N + 1) * sizeof **A); \
		if (tmp_array != NULL) {                             \
			(*N)++;                                      \
			*A = tmp_array;                              \
			(*A)[(*N) - 1] = I;                          \
			RV = 0;                                      \
		} else {                                             \
			RV = -1;                                     \
		}                                                    \
	}

#endif /* ARRAY_H_ */
