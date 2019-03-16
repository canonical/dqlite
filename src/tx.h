#ifndef TX_H_
#define TX_H_

#include <stdbool.h>

#include <sqlite3.h>

enum { TX__PENDING = 0, /* Initial state right after creation */
       TX__WRITING, /* After a non-commit frames command has been applied. */
       TX__WRITTEN, /* After a commit frames command has been applied. */
       TX__UNDONE,  /* After an undo command has been executed. */
       TX__DOOMED   /* The transaction has errored. */
};
struct tx
{
	unsigned long long id;
	sqlite3 *conn;
	bool is_zombie;
	int state;
};

void tx__init(struct tx *tx, unsigned long long id, sqlite3 *conn);
void tx__close(struct tx *tx);

bool tx__is_leader(struct tx *tx);

int tx__frames(struct tx *tx,
	       bool is_begin,
	       int page_size,
	       int n_frames,
	       unsigned *page_numbers,
	       void *pages,
	       unsigned truncate,
	       bool is_commit);

#endif /* TX_H_*/
