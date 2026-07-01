#include "page_size.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

unsigned pageSize(void)
{
#ifdef _WIN32
	SYSTEM_INFO info;
	GetSystemInfo(&info);
	return info.dwPageSize;
#else
	return (unsigned)sysconf(_SC_PAGESIZE);
#endif
}
