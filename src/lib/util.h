#ifndef LIB_UTIL_H_
#define LIB_UTIL_H_

#define LIKELY(x)       __builtin_expect(!!(x),1)
#define UNLIKELY(x)     __builtin_expect(!!(x),0)

#endif /* LIB_UTIL_H_ */
