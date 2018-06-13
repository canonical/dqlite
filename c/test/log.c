#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "log.h"

struct test_log {
	FILE   *stream;
	char   *buffer;
	size_t  size;
};

test_log *test_log_open(){
	test_log *log = (test_log*)malloc(sizeof(test_log));

	if( !log ){
		fprintf(stderr, "\nFailed allocate test log object: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}

	log->stream = open_memstream(
		&log->buffer, &log->size);

	if( !log->stream ){
		fprintf(stderr, "\nFailed to open test log memory stream: %s\n", strerror(errno));
		exit(EXIT_FAILURE);
	}

	return log;
}

FILE *test_log_stream(test_log *log){
	return stdout;
	assert( log );
	assert( log->stream );

	return log->stream;
}

int test_log_is_empty(test_log* log){
	assert( log );
	assert( log->buffer );

	return log->size == 0;
}

char *test_log_output(test_log *log){
	assert( log );
	assert( log->buffer );

	return log->buffer;
}

void test_log_close(test_log *log){
	int err;

	assert( log );
	assert( log->stream );

	err = fclose(log->stream);

	if( err ){
		fprintf(stderr, "\nFailed to close test log memory stream: %s\n", strerror(err));
		exit(EXIT_FAILURE);
	}
}

void test_log_destroy(test_log *log){
	assert( log );
	assert( log->buffer );

	free(log->buffer);
	free(log);
}
