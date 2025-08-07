#include "../include/dqlite.h"

int dqlite_version_number(void)
{
	return DQLITE_VERSION_NUMBER;
}

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)

const char *dqlite_version_string =
	STRINGIFY(DQLITE_VERSION_MAJOR) "."
	STRINGIFY(DQLITE_VERSION_MINOR) "."
	STRINGIFY(DQLITE_VERSION_RELEASE) "@"
#ifdef DQLITE_VERSION_BUILD_TAG
 	DQLITE_VERSION_BUILD_TAG
#else
	"unknown"
#endif
;
