#include "alloc.h"

#include <stdlib.h>
#include <stdnoreturn.h>
#include <string.h>

noreturn void oomAbort(void)
{
	abort();
}

void *mallocChecked(size_t n)
{
	void *p = malloc(n);
	if (p == NULL) {
		oomAbort();
	}
	return p;
}

void *callocChecked(size_t count, size_t n)
{
	void *p = calloc(count, n);
	if (p == NULL) {
		oomAbort();
	}
	return p;
}

char *strdupChecked(const char *s)
{
	char *p = strdup(s);
	if (p == NULL) {
		oomAbort();
	}
	return p;
}

char *strndupChecked(const char *s, size_t n)
{
	char *p = strndup(s, n);
	if (p == NULL) {
		oomAbort();
	}
	return p;
}
